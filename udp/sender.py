import socket
from google.protobuf.json_format import MessageToJson
from ..protobuf.messages_pb2 import ClusterMessageType

from ..log import logger

class UDPEmitter:
    def __init__(self, port: int):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16777216)
        if hasattr(socket, "SO_PRIORITY"):
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, 6)
        self.sock.settimeout(5)
        self.sock.setblocking(False)
        self._default_broadcast_addresss = "255.255.255.255"
        self._last_time_log = 0
        self._moving_avg_time = 0
        self._alpha = 0.1  # Smoothing factor for moving average

    def emit_message(
        self, message: object, addr_and_port: tuple[str, int] | None = None
    ):
        if addr_and_port is None:
            send_addr = self._default_broadcast_addresss
            send_port = self.port
        else:
            send_addr = addr_and_port[0]
            send_port = addr_and_port[1]

        msg_json = MessageToJson(message)
        logger.debug(
            "(SENDING) UDP message to %s:%d\n"
            "\ttype: %s\n"
            "\tmessage_id: %s\n"
            "\tsender_instance_id: %s\n"
            "\trequire_ack: %s",
            send_addr,
            send_port,
            ClusterMessageType.Name(message.header.type),
            message.header.message_id,
            message.header.sender_instance_id,
            message.header.require_ack,
        )
        self.sock.sendto(msg_json.encode(), (send_addr, send_port))

    def emit_buffer(
        self, buffer: bytearray, addr: tuple[str, int] | None = None
    ):
        if addr is None:
            send_addr = self._default_broadcast_addresss
            send_port = self.port
        else:
            send_addr = addr[0]
            send_port = addr[1]

        try:
            # start_time = time.time()
            self.sock.sendto(buffer, (send_addr, send_port))
            # elapsed = time.time() - start_time
            # logger.debug(f"Buffer emit took {elapsed:.6f} seconds")
        except BlockingIOError:
            logger.warning(
                "Socket send operation would block, consider increasing buffer size or adjusting send rate."
            )
        except Exception as e:
            logger.error(f"Error sending buffer: {e}")

    def __del__(self):
        self.sock.close()
