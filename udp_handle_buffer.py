import asyncio
import traceback
import queue
import time
import threading

from .log import logger
from .udp_base import UDPBase, ACKResult
from .udp_base import UDPSingleton
from .env_vars import EnvVars
from .queued import IncomingPacket, IncomingBuffer, OutgoingPacket


class UDPBufferHandler(UDPBase):
    def __init__(
        self, state_loop, incoming_processed_packet_queue: queue.Queue
    ):
        super().__init__(incoming_processed_packet_queue)
        logger.info("Initializing UDP handler")
        self._state_loop = state_loop
        self._last_packet_time: float = time.time()
        UDPSingleton.add_handle_incoming_packet_callback(
            self._handle_incoming_packet
        )
        UDPSingleton.add_outgoing_thread_callback(
            self._outgoing_thread_callback
        )

    def get_incoming_buffer_queue_size(self) -> int:
        return self._incoming_processed_packet_queue.qsize

    def get_time_since_last_packet(self) -> float:
        return time.time() - self._last_packet_time

    def _handle_incoming_packet(self, incoming: IncomingBuffer):
        try:
            if (
                not incoming.get_is_buffer()
                or incoming.get_sender_instance_id()
                == EnvVars.get_instance_index()
            ):
                return

            self._process_buffer(incoming)
            self._last_packet_time = time.time()

        except Exception as e:
            logger.error(
                "Exception occurred while trying to handle incoming packet as a buffer: %s\n%s",
                e,
                traceback.format_exc(),
            )

    def _outgoing_thread_callback(self):
        try:
            UDPSingleton.process_batch_outgoing(
                self.dequeue_outgoing_packet,
                lambda msg: self._emit_byte_buffer(msg),
            )

        except Exception as e:
            logger.error("Send loop error: %s\n%s", e, traceback.format_exc())

    def queue_byte_buffer(self, byte_buffer, instance_id: int | None = None):
        addr = None
        if instance_id is not None:
            addr, port = UDPSingleton.get_cluster_instance_address(instance_id)
            queued_packet = OutgoingPacket(byte_buffer, (addr, port))
        else:
            queued_packet = OutgoingPacket(byte_buffer)
        self.queue_outgoing_packet(queued_packet)

        queue_size = self._outgoing_queue.qsize()
        if queue_size % 1000 == 0:
            logger.debug("Outgoing buffer queue size: %s", queue_size)

    def _process_buffer(self, incoming: IncomingBuffer):
        try:
            self._incoming_processed_packet_queue.put_nowait(incoming)

            queue_size = self._incoming_processed_packet_queue.qsize()
            if queue_size % 1000 == 0:
                logger.debug("Incoming buffer queue size: %s", queue_size)

        except queue.Full:
            logger.warning(f"Message queue full, dropping buffer.")

    def _emit_byte_buffer(self, outgoing_packet: OutgoingPacket):
        # Remove duplicate emission when addr is provided
        if outgoing_packet.optional_addr is not None:
            self._emitter.emit_buffer(
                outgoing_packet.packet, outgoing_packet.optional_addr
            )
        elif EnvVars.get_udp_broadcast():
            self._emitter.emit_buffer(outgoing_packet.packet)
        else:  # Loop through each hostname and emit message directly.
            for (
                instance_id,
                hostname,
                direct_listening_port,
            ) in UDPSingleton.get_cluster_instance_addresses():
                if instance_id == EnvVars.get_instance_index():
                    continue
                self._emitter.emit_buffer(
                    outgoing_packet.packet, (hostname, direct_listening_port)
                )
