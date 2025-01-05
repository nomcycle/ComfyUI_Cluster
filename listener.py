import socket
import json
import traceback
import os
from .log import logger
from .env_vars import EnvVars

class UDPListener:
    def __init__(self, host: str, port: int, message_callback):
        self.host = host
        self.port = port
        self.message_callback = message_callback
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self._local_ips: [str] = None
        logger.debug("UDP listener bound to %s:%d", host, port)

    def get_cached_local_addreses(self):
        if self._local_ips is None:
            interfaces = socket.getaddrinfo(socket.gethostname(), None)
            self._local_ips = [interface[4][0] for interface in interfaces]
            logger.debug("Local IP addresses: %s", self._local_ips)
        return self._local_ips

    def poll(self):
        data, addr = self.sock.recvfrom(65535)  # Max UDP packet size
        sender_addr = addr[0]

        message = json.loads(data.decode())

        header = message.get('header', None)
        if not header:
            raise ValueError("Missing message header")
        if EnvVars.get_single_host():
            process_id = header.get('processId', -1)
            if process_id == -1:
                raise ValueError("Header missing process_id")
            if process_id == os.getpid():
                return

        if not EnvVars.get_single_host() and sender_addr in self.get_cached_local_addreses():
            return

        logger.debug("(RECEIVED) UDP message from %s:\n%s", sender_addr, json.dumps(message, indent=2))
        
        if self.message_callback:
            self.message_callback(header, message, sender_addr)
                
    def __del__(self):
        self.sock.close()