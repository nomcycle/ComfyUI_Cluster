import socket
import json
from .log import logger

class UDPListener:
    def __init__(self, host='0.0.0.0', port=9988, message_callback=None):
        self.host = host
        self.port = port
        self.message_callback = message_callback
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        logger.info(f"Initialized UDP broadcast listener on {host}:{port}")

    def poll(self):
        try:
            data, addr = self.sock.recvfrom(65535)  # Max UDP packet size
            message = json.loads(data.decode())
            logger.info(f"Received message from {addr}:\n{message}")
            
            if self.message_callback:
                self.message_callback(message, addr)
                
        except Exception as e:
            logger.error(f"Error receiving message: {e}")

    def __del__(self):
        self.sock.close()