import asyncio
from enum import Enum, auto
import threading
from typing import Dict
from ...queued import IncomingMessage
from ..state_result import StateResult

import numpy as np
import math
from ...log import logger

from .sync_handler import SyncHandler
from ...env_vars import EnvVars

from google.protobuf.json_format import ParseDict
from ...protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterDistributeBufferBegin, 
    ClusterDistributeBufferAck, ClusterBufferType, ClusterMessageHeader,
    ClusterDistributeBufferResend
)

class OtherInstanceState(Enum):
    AWAITING_CHUNKS = auto()
    REQUESTED_RESEND = auto()
    COMPLETE_BUFFER  = auto()

from ...udp_handle_message import UDPMessageHandler
from ...udp_handle_buffer import UDPBufferHandler

class Emitter(SyncHandler):
    def __init__(self,
        udp_message_handler: UDPMessageHandler,
        udp_buffer_handler: UDPBufferHandler,
        asyncio_loop: asyncio.AbstractEventLoop,
        all_instances_received_buffer: asyncio.Future,
        buffer: bytes):

        super().__init__(udp_message_handler, udp_buffer_handler, asyncio_loop)

        self._thread_lock = threading.Lock()
        self._all_instances_received_buffer: asyncio.Future = all_instances_received_buffer

        self._this_instance_dependency_byte_buffer: bytes = buffer
        self._this_instance_dependency_buffer_type: int = -1
        self._this_instance_dependency_chunks: Dict[int, bytes] = {}  # chunk_id -> chunk_data
        self._sent_begin_buffer: bool = False

        # Track state and chunks for each instance
        self._instance_states: Dict[int, OtherInstanceState] = {
            i: OtherInstanceState.AWAITING_CHUNKS
            for i in range(EnvVars.get_instance_count())
            if i != EnvVars.get_instance_index()
        }
        self._instance_chunk_bitfields: Dict[int, np.ndarray] = {}
        self._expected_chunk_ids: Dict[int, list] = {}
        self._received_acks: set = set()

        self._prepare_buffers(buffer)
    
    async def begin(self):
        instance_index = EnvVars.get_instance_index()
        byte_buffer_size = len(self._this_instance_dependency_byte_buffer)
        chunk_count = math.ceil(byte_buffer_size / (SyncHandler.UDP_MTU - SyncHandler.HEADER_SIZE))

        message = ClusterDistributeBufferBegin(
            header=ClusterMessageHeader(
                type=ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN,
                require_ack=True
            ),
            instance_index=instance_index,
            buffer_type=self._this_instance_dependency_buffer_type,
            buffer_byte_size=byte_buffer_size,
            chunk_count=chunk_count
        )

        await self._udp_message_handler.send_and_wait_thread_safe(message)
        self._sent_begin_buffer = True

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        with self._thread_lock:
            if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
                if self._instance_states.get(incoming_message.sender_instance_id) != OtherInstanceState.COMPLETE_BUFFER:
                    resend_msg = ParseDict(incoming_message.message, ClusterDistributeBufferResend())
                    missing_bits = np.unpackbits(np.frombuffer(resend_msg.missing_chunk_ids, dtype=np.uint8)).astype(bool)
                    expected_length = len(self._expected_chunk_ids.get(EnvVars.get_instance_index(), []))
                    missing_bits = missing_bits[:expected_length]
                    logger.debug("Received resend request for %d chunks", len(np.nonzero(missing_bits)[0]))
                    self._instance_chunk_bitfields[incoming_message.sender_instance_id] = ~missing_bits
                    self._instance_states[incoming_message.sender_instance_id] = OtherInstanceState.REQUESTED_RESEND

            if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
                logger.debug("Received buffer ACK")
                ack_msg = ParseDict(incoming_message.message, ClusterDistributeBufferAck())
                self._received_acks.add(ack_msg.instance_index)
                self._instance_states[ack_msg.instance_index] = OtherInstanceState.COMPLETE_BUFFER
                self._instance_chunk_bitfields[ack_msg.instance_index] = np.ones(len(self._instance_chunk_bitfields.get(ack_msg.instance_index, [])), dtype=np.bool_)

                # Check if all instances have completed receiving the buffer
                all_complete = True
                for instance_id in range(EnvVars.get_instance_count()):
                    if instance_id != EnvVars.get_instance_index() and self._instance_states[instance_id] != OtherInstanceState.COMPLETE_BUFFER:
                        all_complete = False
                        break
                
                if all_complete:
                    logger.debug("All instances have completed receiving the buffer")
                    self._async_loop.call_soon_threadsafe(self._all_instances_received_buffer.set_result, None)

    def _create_chunks(self, byte_buffer):
        chunk_size = SyncHandler.UDP_MTU - SyncHandler.HEADER_SIZE
        chunk_count = math.ceil(len(byte_buffer) / (SyncHandler.UDP_MTU - SyncHandler.HEADER_SIZE))
        chunk_ids = list(range(chunk_count))
        self._this_instance_dependency_chunks = {}

        buffer_view = memoryview(byte_buffer)

        for i in range(chunk_count):
            start = i * chunk_size
            end = min(start + chunk_size, len(byte_buffer))
            chunk_data = buffer_view[start:end].tobytes()
            chunk_id = chunk_ids[i]
            self._this_instance_dependency_chunks[chunk_id] = chunk_data

        self._expected_chunk_ids[EnvVars.get_instance_index()] = chunk_ids
        self._instance_chunk_bitfields[EnvVars.get_instance_index()] = np.ones(len(self._this_instance_dependency_chunks), dtype=bool)

    def _prepare_buffers(self, byte_buffer: bytes):
        self._this_instance_dependency_byte_buffer = byte_buffer
        self._create_chunks(self._this_instance_dependency_byte_buffer)

    def _has_received_all_instance_resend_requests(self) -> bool:
        for instance_id in range(EnvVars.get_instance_count()):
            if instance_id == EnvVars.get_instance_index():
                continue
            if instance_id not in self._instance_states or self._instance_states[instance_id] not in [OtherInstanceState.REQUESTED_RESEND, OtherInstanceState.AWAITING_CHUNKS]:
                return False
        return True

    async def tick(self):
        if not self._sent_begin_buffer:
            return

        # Only send chunks if we have any to send and all instances have sent resend requests
        if not self._has_received_all_instance_resend_requests():
            return

        for instance_id in range(EnvVars.get_instance_count()):
            if self._instance_states.get(instance_id) == OtherInstanceState.REQUESTED_RESEND:
                self._instance_states[instance_id] = OtherInstanceState.AWAITING_CHUNKS

        # Get all instances that have acked
        acked_instances = []
        for i in range(EnvVars.get_instance_count()):
            if i in self._instance_chunk_bitfields and len(self._instance_chunk_bitfields[i]) > 0:
                acked_instances.append(i)
                
        if not acked_instances:
            return

        # Create a combined bitfield of all acked chunks
        chunk_count = len(self._this_instance_dependency_chunks)
        all_instance_missing_chunks = np.zeros(chunk_count, dtype=np.bool_)
        
        # For each instance that has acked, OR their ack bitfield
        for instance_index in acked_instances:
            all_instance_missing_chunks |= ~self._instance_chunk_bitfields[instance_index]

        # Find indices where chunks haven't been acked
        missing_chunks = np.nonzero(all_instance_missing_chunks)[0]
        
        if missing_chunks.size > 0:
            logger.debug(f"Resending {len(missing_chunks)} chunks that weren't acked by all instances")
            buffer_flag = int(123456789).to_bytes(4, byteorder='big')
            instance_id_bytes = int(EnvVars.get_instance_index()).to_bytes(4, byteorder='big')
            
            for chunk_id in missing_chunks:
                chunk_id_bytes = int(chunk_id).to_bytes(4, byteorder='big')
                chunk_data = self._this_instance_dependency_chunks[chunk_id]
                self._emit_byte_chunk(buffer_flag, instance_id_bytes, chunk_id_bytes, chunk_data)

    def _emit_byte_chunk(self, buffer_flag: bytes, sender_instance_id_bytes: bytes, chunk_id_bytes: bytes, chunk_data: bytes, to_instance_id: int | None = None):
        chunk_with_id = buffer_flag + sender_instance_id_bytes + chunk_id_bytes + chunk_data
        self._udp_buffer_handler.queue_byte_buffer(chunk_with_id, to_instance_id)