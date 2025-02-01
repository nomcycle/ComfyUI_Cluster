import socket
import json
import time
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
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16777216)
        if hasattr(socket, 'SO_PRIORITY'):
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)
        self.sock.settimeout(5)
        self.sock.setblocking(False)
        self._default_broadcast_addresss = '255.255.255.255'
        self._last_time_log = 0
        self._moving_avg_time = 0
        self._alpha = 0.1  # Smoothing factor for moving average

    def emit_message(self, message: object, addr: str | None = None):
        if addr is None:
            addr = self._default_broadcast_addresss

        msg_json = MessageToJson(message)
        logger.debug("(SENDING) UDP message to %s:%d\n"
                    "\ttype: %s\n"
                    "\tmessage_id: %s\n" 
                    "\tsender_instance_id: %s\n"
                    "\trequire_ack: %s",
                    addr, self.port,
                    ClusterMessageType.Name(message.header.type),
                    message.header.message_id,
                    message.header.sender_instance_id,
                    message.header.require_ack)
        self.sock.sendto(msg_json.encode(), (addr, self.port))

    def emit_buffer(self, buffer: bytes, addr: str | None = None):
        if addr is None:
            addr = self._default_broadcast_addresss
        try:
            # start_time = time.time()
            self.sock.sendto(buffer, (addr, self.port))
            # elapsed = time.time() - start_time
            # logger.debug(f"Buffer emit took {elapsed:.6f} seconds")
        except BlockingIOError:
            logger.warning("Socket send operation would block, consider increasing buffer size or adjusting send rate.")
        except Exception as e:
            logger.error(f"Error sending buffer: {e}")

    def __del__(self):
        self.sock.close()