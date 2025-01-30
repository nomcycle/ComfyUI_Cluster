import socket
import json
import traceback
from google.protobuf.json_format import MessageToJson
from .protobuf.messages_pb2 import (ClusterMessageType, ClusterAck)
from .log import logger

class UDPEmitter:
    def __init__(self, port: int):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(5)
        # self.sock.setblocking(False)

    def emit_message(self, message: object, addr: str | None = None):
        if addr is None:
            addr = '255.255.255.255'

        msg_json = MessageToJson(message)
        logger.debug("(SENDING) UDP message to %s:%d\n"
                    "\ttype: %s\n"
                    "\tmessage_id: %s\n" 
                    "\tsender_instance_id: %s\n"
                    "\tprocess_id: %s\n"
                    "\trequire_ack: %s",
                    addr, self.port,
                    ClusterMessageType.Name(message.header.type),
                    message.header.message_id,
                    message.header.sender_instance_id,
                    message.header.process_id,
                    message.header.require_ack)
        self.sock.sendto(msg_json.encode(), (addr, self.port))

    def emit_buffer(self, bytes: bytes, addr: str | None = None):
        if addr is None:
            addr = '255.255.255.255'

        # logger.debug("(SENDING) UDP buffer to %s:%d", addr, self.port)
        self.sock.sendto(bytes, (addr, self.port))

    def __del__(self):
        self.sock.close()