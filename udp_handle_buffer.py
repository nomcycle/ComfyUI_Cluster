import asyncio
import traceback

from .log import logger
from .udp_base import UDPBase
from .udp_base import UDPSingleton
from .env_vars import EnvVars
from .queued import IncomingPacket, OutgoingPacket
    
class ACKResult:
    def __init__(self, success: bool, error_msg: str = None):
        self.success = success
        self.error_msg = error_msg

class UDPBufferHandler(UDPBase):
    def __init__(self, state_loop, incoming_processed_packet_queue):
        super().__init__(incoming_processed_packet_queue)
        logger.info("Initializing UDP handler")
        self._state_loop = state_loop
        UDPSingleton.add_handle_incoming_packet_callback(self._handle_incoming_packet)
        UDPSingleton.add_outgoing_thread_callback(self._outgoing_thread_callback)

    def _handle_incoming_packet(self, incoming_packet: IncomingPacket):
        try:
            if not incoming_packet.get_is_buffer():
                return
            self._process_buffer(incoming_packet)
        except Exception as e:
            logger.error("Exception occurred while trying to handle incoming packet as a buffer: %s\n%s", e, traceback.format_exc())
    
    def _outgoing_thread_callback(self):
        try:
            UDPSingleton.process_batch_outgoing(
                self._outgoing_queue,
                lambda msg: self._emit_byte_buffer(msg.packet, msg.optional_addr))

        except Exception as e:
            logger.error("Send loop error: %s\n%s", e, traceback.format_exc())
    
    def queue_byte_buffer(self, byte_buffer, addr: str = None):
        self._outgoing_queue.put(OutgoingPacket(byte_buffer, addr))

    def _process_buffer(self, incoming_packet: IncomingPacket):
        self._incoming_processed_packet_queue.put(incoming_packet)

    def _emit_byte_buffer(self, byte_buffer: bytes, addr: str = None):
        self._emitter.emit_buffer(byte_buffer, addr)
        if addr is not None:
            self._emitter.emit_buffer(byte_buffer, addr)
        elif EnvVars.get_udp_broadcast():
            self._emitter.emit_buffer(byte_buffer)
        else: # Loop through each hostname and emit message directly.
            for hostname in UDPSingleton.get_cluster_instance_addresses():
                self._emitter.emit_buffer(byte_buffer, hostname)
