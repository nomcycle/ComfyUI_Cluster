import socket
import json
import traceback
from google.protobuf.json_format import MessageToJson
from .log import logger

class UDPSender:
    def __init__(self, port: int):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(5)

    def send(self, message, addr: str = None):
        if addr is None:
            addr = '255.255.255.255'

        msg_json = MessageToJson(message)
        # logger.debug("(SENDING) UDP message to %s:%d:\n%s", addr, self.port, json.dumps(json.loads(msg_json), indent=2))
        self.sock.sendto(msg_json.encode(), (addr, self.port))

    def send_bytes(self, bytes, addr:str = None):
        if addr is None:
            addr = '255.255.255.255'

        # logger.debug("(SENDING) UDP buffer to %s:%d", addr, self.port)
        self.sock.sendto(bytes, (addr, self.port))

    def __del__(self):
        self.sock.close()