import traceback
import queue
import time
from typing import Optional, Callable

from ..log import logger
from ..env_vars import EnvVars
from .udp_base import UDPBase, IMessageHandler
from .udp_base import UDPSingleton
from .queued import IncomingBuffer, OutgoingPacket


class IBufferValidator:
    """
    Interface for validating incoming buffer packets.
    """
    def validate(self, incoming: IncomingBuffer) -> bool:
        """
        Validate an incoming buffer packet.
        Returns True if the buffer is valid and should be processed.
        """
        pass


class IBufferProcessor:
    """
    Interface for processing buffer packets.
    """
    def process_buffer(self, incoming: IncomingBuffer) -> None:
        """
        Process an incoming buffer packet.
        """
        pass


class IBufferSender:
    """
    Interface for sending buffer packets.
    """
    def queue_buffer(self, buffer, instance_id: Optional[int] = None) -> None:
        """
        Queue a buffer for sending.
        """
        pass
        
    def emit_buffer(self, outgoing_packet: OutgoingPacket) -> None:
        """
        Emit a buffer packet.
        """
        pass


class BufferValidator(IBufferValidator):
    """
    Validates incoming buffer packets.
    """
    def __init__(self, instance_id: int):
        self._instance_id = instance_id
        
    def validate(self, incoming: IncomingBuffer) -> bool:
        """
        Validate an incoming buffer packet.
        """
        return (
            incoming.get_is_buffer() and 
            incoming.get_sender_instance_id() != self._instance_id
        )


class BufferProcessor(IBufferProcessor):
    """
    Processes buffer packets.
    """
    def __init__(self, incoming_queue: queue.Queue):
        self._incoming_queue = incoming_queue
        
    def process_buffer(self, incoming: IncomingBuffer) -> None:
        """
        Process an incoming buffer packet.
        """
        try:
            self._incoming_queue.put_nowait(incoming)

            queue_size = self._incoming_queue.qsize()
            if queue_size % 1000 == 0:
                logger.debug("Incoming buffer queue size: %s", queue_size)

        except queue.Full:
            logger.warning("Message queue full, dropping buffer.")


class BufferSender(IBufferSender):
    """
    Sends buffer packets.
    """
    def __init__(self, 
                instance_id: int, 
                emitter,
                queue_outgoing_fn: Callable):
        self._instance_id = instance_id
        self._emitter = emitter
        self._queue_outgoing_fn = queue_outgoing_fn
        
    def queue_buffer(self, buffer, instance_id: Optional[int] = None) -> None:
        """
        Queue a buffer for sending.
        """
        if instance_id is not None:
            addr, port = UDPSingleton.get_cluster_instance_address(instance_id)
            queued_packet = OutgoingPacket(buffer, (addr, port))
        else:
            queued_packet = OutgoingPacket(buffer)
            
        self._queue_outgoing_fn(queued_packet)
        
    def emit_buffer(self, outgoing_packet: OutgoingPacket) -> None:
        """
        Emit a buffer packet.
        """
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
                if instance_id == self._instance_id:
                    continue
                self._emitter.emit_buffer(
                    outgoing_packet.packet, (hostname, direct_listening_port)
                )


class UDPBufferHandler(UDPBase):
    """
    Handles UDP buffer packets using the refactored components.
    """
    def __init__(
        self, state_loop, incoming_processed_packet_queue: queue.Queue
    ):
        super().__init__(incoming_processed_packet_queue)
        logger.info("Initializing UDP buffer handler")
        self._state_loop = state_loop
        self._instance_id = EnvVars.get_instance_index()
        self._last_packet_time: float = time.time()
        
        # Create specialized components
        self._validator = BufferValidator(self._instance_id)
        self._processor = BufferProcessor(incoming_processed_packet_queue)
        self._sender = BufferSender(
            self._instance_id,
            self._emitter,
            self.queue_outgoing_packet
        )
        
        # Register callbacks
        UDPSingleton.add_handle_incoming_packet_callback(
            self._handle_incoming_packet
        )
        UDPSingleton.add_outgoing_thread_callback(
            self._outgoing_thread_callback
        )

    def get_incoming_buffer_queue_size(self) -> int:
        """
        Get the size of the incoming buffer queue.
        """
        return self._incoming_processed_packet_queue.qsize

    def get_time_since_last_packet(self) -> float:
        """
        Get the time since the last packet was received.
        """
        return time.time() - self._last_packet_time

    def _handle_incoming_packet(self, incoming: IncomingBuffer):
        """
        Handle an incoming packet using the validator and processor components.
        """
        try:
            # Skip processing if not a valid buffer
            if not self._validator.validate(incoming):
                return

            # Process the buffer
            self._processor.process_buffer(incoming)
            self._last_packet_time = time.time()

        except Exception as e:
            logger.error(
                "Exception occurred while trying to handle incoming packet as a buffer: %s\n%s",
                e,
                traceback.format_exc(),
            )

    def _outgoing_thread_callback(self):
        """
        Callback for processing outgoing buffers.
        """
        try:
            UDPSingleton.process_batch_outgoing(
                self.dequeue_outgoing_packet,
                self._sender.emit_buffer,
            )

        except Exception as e:
            logger.error("Send loop error: %s\n%s", e, traceback.format_exc())

    def queue_byte_buffer(self, byte_buffer, instance_id: Optional[int] = None):
        """
        Queue a byte buffer for sending.
        """
        self._sender.queue_buffer(byte_buffer, instance_id)
        
        # Log queue size periodically - use the proper method
        queue_size = self.get_outgoing_queue_size()
        if queue_size % 1000 == 0:
            logger.debug("Outgoing buffer queue size: %s", queue_size)
