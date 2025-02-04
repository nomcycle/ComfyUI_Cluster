import asyncio
import traceback
import queue
import time
import threading

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
    def __init__(self, state_loop, incoming_processed_packet_queue: queue.Queue):
        super().__init__(incoming_processed_packet_queue)
        logger.info("Initializing UDP handler")
        self._state_loop = state_loop
        self._last_packet_time = 0
        UDPSingleton.add_handle_incoming_packet_callback(self._handle_incoming_packet)
        UDPSingleton.add_outgoing_thread_callback(self._outgoing_thread_callback)

    def get_incoming_buffer_queue_size(self):
        return self._incoming_processed_packet_queue.qsize

    def _handle_incoming_packet(self, incoming_packet: IncomingPacket):
        try:
            if not incoming_packet.get_is_buffer():
                return
                
            self._process_buffer(incoming_packet)
            self._last_packet_time = time.time()

        except Exception as e:
            logger.error("Exception occurred while trying to handle incoming packet as a buffer: %s\n%s", e, traceback.format_exc())
    
    def _outgoing_thread_callback(self):
        try:
            UDPSingleton.process_batch_outgoing(
                self.dequeue_outgoing_packet,
                lambda msg: self._emit_byte_buffer(msg.packet, msg.optional_addr))

        except Exception as e:
            logger.error("Send loop error: %s\n%s", e, traceback.format_exc())
    
    def queue_byte_buffer(self, byte_buffer, instance_id: int | None = None):
        addr = None
        if instance_id is not None:
            addr = UDPSingleton.get_cluster_instance_address(instance_id)
        queued_packet = OutgoingPacket(byte_buffer, addr)
        self.queue_outgoing_packet(queued_packet)

        queue_size = self._outgoing_queue.qsize()
        if queue_size % 1000 == 0:
            logger.debug('Outgoing buffer queue size: %s', queue_size)

    def _process_buffer(self, incoming_packet: IncomingPacket):
        try:
            self._incoming_processed_packet_queue.put_nowait(incoming_packet)
            
            queue_size = self._incoming_processed_packet_queue.qsize()
            if queue_size % 1000 == 0:
                logger.debug('Incoming buffer queue size: %s', queue_size)

        except queue.Full:
            logger.warning(f"Message queue full, dropping buffer.")

    def _emit_byte_buffer(self, byte_buffer: bytes, addr: str | None = None):
        # Remove duplicate emission when addr is provided
        if addr is not None:
            self._emitter.emit_buffer(byte_buffer, addr)
        elif EnvVars.get_udp_broadcast():
            self._emitter.emit_buffer(byte_buffer)
        else: # Loop through each hostname and emit message directly.
            for instance_id, hostname in UDPSingleton.get_cluster_instance_addresses():
                if instance_id == EnvVars.get_instance_index():
                    continue
                self._emitter.emit_buffer(byte_buffer, hostname)
