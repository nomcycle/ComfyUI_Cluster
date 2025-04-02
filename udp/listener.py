import socket
from ..log import logger


class UDPListener:
    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16777216)

        actual_buffer_size = self.sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF
        )
        logger.debug(
            f"UDP receive buffer size: {actual_buffer_size / 1024 / 1024:.2f} MB"
        )

        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_PRIORITY"):
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)
        self.sock.setblocking(False)

        self.sock.bind((self._host, self._port))
        logger.debug("UDP listener bound to %s:%d", host, port)

    def poll(self):
        try:
            packet, addr = self.sock.recvfrom(65535)  # Max UDP packet size
            sender_addr = addr[0]
            return packet, sender_addr
        except BlockingIOError:
            # No data available yet
            return None, None

    def __del__(self):
        self.sock.close()
