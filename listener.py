import socket
from .log import logger

class UDPListener:
    def __init__(self, host: str, port: int):

        self._host = host
        self._port = port

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        receive_buffer_size = 16 * 1024 * 1024  # 16MB
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer_size)
        
        actual_buffer_size = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        logger.debug(f"UDP receive buffer size: {actual_buffer_size / 1024 / 1024:.2f} MB")
        
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        self.sock.bind((self._host, self._port))
        logger.debug("UDP listener bound to %s:%d", host, port)

    def poll(self):
        packet, addr = self.sock.recvfrom(65535)  # Max UDP packet size
        sender_addr = addr[0]
        return packet, sender_addr
                
    def __del__(self):
        self.sock.close()