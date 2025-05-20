from ...log import logger
from ...udp.queued import IncomingMessage
from .sync_handler import SyncHandler
from ...env_vars import EnvVars
from ...udp.expected_msg import BEGIN_BUFFER_EXPECTED_MSG_KEY
from ...udp.udp_handle_message import UDPMessageHandler
from ...udp.udp_handle_buffer import UDPBufferHandler


import asyncio
from enum import Enum, auto
import threading
from typing import Dict, List, Optional, Set

import numpy as np
import math

from google.protobuf.json_format import ParseDict
from ...protobuf.messages_pb2 import (
    ClusterMessageType,
    ClusterDistributeBufferBegin,
    ClusterDistributeBufferAck,
    ClusterMessageHeader,
    ClusterDistributeBufferResend,
)


class OtherInstanceState(Enum):
    AWAITING_CHUNKS = auto()
    REQUESTED_RESEND = auto()
    COMPLETE_BUFFER = auto()


class Emitter(SyncHandler):
    def __init__(
        self,
        udp_message_handler: UDPMessageHandler,
        udp_buffer_handler: UDPBufferHandler,
        asyncio_loop: asyncio.AbstractEventLoop,
        all_instances_received_buffer: asyncio.Future,
        buffer: bytes,
        to_instance_ids: Optional[int | List[int]] = None,
    ):
        super().__init__(udp_message_handler, udp_buffer_handler, asyncio_loop)

        self._thread_lock = threading.Lock()
        self._all_instances_received_buffer = all_instances_received_buffer

        # Buffer data
        self._this_instance_dependency_byte_buffer = buffer
        self._this_instance_dependency_buffer_type = -1
        self._this_instance_dependency_chunks: Dict[int, bytes] = {}  # chunk_id -> chunk_data
        self._sent_begin_buffer = False

        # Process target instance IDs
        if to_instance_ids is None:
            to_instance_ids = list(range(EnvVars.get_instance_count()))
        elif isinstance(to_instance_ids, int):
            to_instance_ids = [to_instance_ids]
            
        # Remove self from target instances
        instance_index = EnvVars.get_instance_index()
        if instance_index in to_instance_ids:
            to_instance_ids.remove(instance_index)
            
        self._to_instance_ids = to_instance_ids

        # Initialize instance states
        self._instance_states: Dict[int, OtherInstanceState] = {
            i: OtherInstanceState.AWAITING_CHUNKS
            for i in to_instance_ids
        }

        # Tracking for chunk delivery
        self._instance_chunk_bitfields: Dict[int, np.ndarray] = {}
        self._accumulated_missing_chunks = np.array([], dtype=np.bool_)
        self._expected_chunk_ids: Dict[int, list] = {}
        self._received_acks: Set[int] = set()

        # Prepare buffer chunks
        self._prepare_buffers(buffer)

    async def begin(self):
        await self._fence_instances()

        instance_index = EnvVars.get_instance_index()
        byte_buffer_size = len(self._this_instance_dependency_byte_buffer)
        chunk_size = SyncHandler.UDP_MTU - SyncHandler.HEADER_SIZE
        chunk_count = math.ceil(byte_buffer_size / chunk_size)

        # Create begin message
        message = ClusterDistributeBufferBegin(
            header=ClusterMessageHeader(
                type=ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN,
                require_ack=True,
            ),
            instance_index=instance_index,
            buffer_type=self._this_instance_dependency_buffer_type,
            buffer_byte_size=byte_buffer_size,
            chunk_count=chunk_count,
        )

        await self._udp_message_handler.send_expected_message_thread_safe(
            message, BEGIN_BUFFER_EXPECTED_MSG_KEY
        )
        self._sent_begin_buffer = True

    async def handle_message(
        self, current_state: int, incoming_message: IncomingMessage
    ) -> None:
        with self._thread_lock:
            # Handle resend requests
            if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
                await self._handle_resend_request(incoming_message)
                
            # Handle acknowledgments
            elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
                await self._handle_ack(incoming_message)
                
    async def _handle_resend_request(self, incoming_message: IncomingMessage) -> None:
        sender_id = incoming_message.sender_instance_id
        
        # Skip if this instance has already completed receiving the buffer
        if self._instance_states.get(sender_id) == OtherInstanceState.COMPLETE_BUFFER:
            return
            
        # Parse the resend message
        resend_msg = ParseDict(
            incoming_message.message,
            ClusterDistributeBufferResend(),
        )

        # Get the window information
        window_start = resend_msg.window_start
        window_size = resend_msg.window_size
        window_end = window_start + window_size

        # Unpack the bitfield of missing chunks
        missing_bits = np.unpackbits(
            np.frombuffer(resend_msg.missing_chunk_ids, dtype=np.uint8)
        ).astype(bool)

        # Ensure the unpacked bits match the expected window size
        missing_bits = missing_bits[:window_size]

        # Initialize the full bitfield if it doesn't exist
        expected_length = len(
            self._expected_chunk_ids.get(EnvVars.get_instance_index(), [])
        )
        if sender_id not in self._instance_chunk_bitfields:
            self._instance_chunk_bitfields[sender_id] = np.ones(expected_length, dtype=bool)

        # Update the instance's bitfield for the window - mark missing chunks
        instance_chunk_bitfield = self._instance_chunk_bitfields[sender_id]
        # Set to 0 where missing_bits is 1 (chunk is missing)
        instance_chunk_bitfield[window_start:window_end] = ~missing_bits

        # Initialize accumulated missing chunks if needed
        if len(self._accumulated_missing_chunks) == 0:
            self._accumulated_missing_chunks = np.zeros(expected_length, dtype=bool)

        # Update accumulated missing chunks for this window
        self._accumulated_missing_chunks[window_start:window_end] |= missing_bits

        # Update instance state
        self._instance_states[sender_id] = OtherInstanceState.REQUESTED_RESEND
        
    async def _handle_ack(self, incoming_message: IncomingMessage) -> None:
        logger.debug('Received buffer ACK')
        
        # Parse the ACK message
        ack_msg = ParseDict(incoming_message.message, ClusterDistributeBufferAck())
        instance_index = ack_msg.instance_index
        
        # Record the ACK
        self._received_acks.add(instance_index)
        self._instance_states[instance_index] = OtherInstanceState.COMPLETE_BUFFER
        
        # Mark all chunks as received for this instance
        bitfield_length = len(self._instance_chunk_bitfields.get(instance_index, []))
        self._instance_chunk_bitfields[instance_index] = np.ones(bitfield_length, dtype=np.bool_)

        # Check if all instances have completed receiving the buffer
        all_complete = all(
            self._instance_states[instance_id] == OtherInstanceState.COMPLETE_BUFFER
            for instance_id in self._to_instance_ids
        )

        if all_complete:
            logger.debug('All instances have completed receiving the buffer')
            self._async_loop.call_soon_threadsafe(
                self._all_instances_received_buffer.set_result, None
            )

    def _create_chunks(self, byte_buffer: bytes) -> None:
        chunk_size = SyncHandler.UDP_MTU - SyncHandler.HEADER_SIZE
        chunk_count = math.ceil(len(byte_buffer) / chunk_size)
        chunk_ids = list(range(chunk_count))
        self._this_instance_dependency_chunks = {}

        # Use memoryview for efficient slicing
        buffer_view = memoryview(byte_buffer)

        # Create chunks from buffer
        for i in range(chunk_count):
            start = i * chunk_size
            end = min(start + chunk_size, len(byte_buffer))
            chunk_data = buffer_view[start:end].tobytes()
            self._this_instance_dependency_chunks[i] = chunk_data

        # Track expected chunks
        self._expected_chunk_ids[EnvVars.get_instance_index()] = chunk_ids
        self._instance_chunk_bitfields[EnvVars.get_instance_index()] = np.ones(
            len(self._this_instance_dependency_chunks), dtype=bool
        )

    def _prepare_buffers(self, byte_buffer: bytes) -> None:
        self._this_instance_dependency_byte_buffer = byte_buffer
        self._create_chunks(byte_buffer)

    def _has_received_all_instance_resend_requests(self) -> bool:
        '''Check if all instances have either requested resends or completed the buffer.'''
        valid_states = [
            OtherInstanceState.REQUESTED_RESEND,
            OtherInstanceState.COMPLETE_BUFFER,
        ]

        return all(
            self._instance_states.get(instance_id) in valid_states
            for instance_id in self._to_instance_ids
        )

    async def tick(self) -> None:
        if not self._sent_begin_buffer:
            return

        # Only send chunks if we have any to send and all instances have sent resend requests
        if not self._has_received_all_instance_resend_requests():
            return

        # Reset state for instances that requested resends
        for instance_id in self._to_instance_ids:
            if self._instance_states.get(instance_id) == OtherInstanceState.REQUESTED_RESEND:
                self._instance_states[instance_id] = OtherInstanceState.AWAITING_CHUNKS

        # Get all instances that have acked
        acked_instances = [
            i for i in self._to_instance_ids 
            if i in self._instance_chunk_bitfields and len(self._instance_chunk_bitfields[i]) > 0
        ]

        if not acked_instances:
            return

        # Find indices where chunks haven't been acked
        if len(self._accumulated_missing_chunks) == 0:
            return

        missing_chunks = np.nonzero(self._accumulated_missing_chunks)[0]

        if missing_chunks.size > 0:
            logger.debug('Resending %d chunks that weren\'t acked by all instances', len(missing_chunks))
            
            # Prepare header data
            buffer_flag = int(123456789).to_bytes(4, byteorder='big')
            instance_id_bytes = int(EnvVars.get_instance_index()).to_bytes(4, byteorder='big')

            # Send each missing chunk
            for chunk_id in missing_chunks:
                chunk_id_bytes = int(chunk_id).to_bytes(4, byteorder='big')
                chunk_data = self._this_instance_dependency_chunks[chunk_id]
                self._emit_byte_chunk(
                    buffer_flag, instance_id_bytes, chunk_id_bytes, chunk_data
                )

            # Assume that all chunks are received (they all won't be, but receiver will remind us)
            self._accumulated_missing_chunks[missing_chunks] = False

    def _emit_byte_chunk(
        self,
        buffer_flag: bytes,
        sender_instance_id_bytes: bytes,
        chunk_id_bytes: bytes,
        chunk_data: bytes,
        to_instance_id: Optional[int] = None,
    ) -> None:
        # Combine all parts into a single buffer
        chunk_with_id = (
            buffer_flag
            + sender_instance_id_bytes
            + chunk_id_bytes
            + chunk_data
        )
        self._udp_buffer_handler.queue_byte_buffer(
            chunk_with_id, to_instance_id
        )
