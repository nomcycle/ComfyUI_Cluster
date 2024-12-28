import socket
import json
from google.protobuf.json_format import MessageToJson
from .log import logger

class UDPSender:
    def __init__(self, port=9988, timeout=5):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(timeout)

    def send(self, message):
        try:
            broadcast_addr = '255.255.255.255'
            msg_json = MessageToJson(message)
            logger.info(f"Broadcasting message to {broadcast_addr}:{self.port}:\n{msg_json}")
            self.sock.sendto(msg_json.encode(), (broadcast_addr, self.port))
            return True
        except Exception as e:
            logger.error(f"Error broadcasting message: {e}")
            return False

    def __del__(self):
        self.sock.close()