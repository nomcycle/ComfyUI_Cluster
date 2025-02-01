from typing import Dict, List, TYPE_CHECKING

import asyncio
import random
import numpy as np
import torch
import math
import threading
import time
from enum import Enum, auto

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterDistributeBufferBegin, 
    ClusterDistributeBufferAck, ClusterBufferType, ClusterMessageHeader,
    ClusterDistributeBufferAllSent, ClusterDistributeBufferResend
)

from .state_handler import StateHandler
from .state_result import StateResult
from ..instance import ThisInstance
from ..env_vars import EnvVars

from ..queued import IncomingMessage

# Constants
HEADER_SIZE = 8  # 4 bytes each for buffer flag and chunk id
UDP_MTU = 1460

class ThisInstanceState(Enum):
    AWAITING_SEND_BEGIN_BUFFER = auto()
    EMITTING = auto()
    AWAITING_CHUNKS = auto()
    ALL_DEPENDENCIES_RESOLVED = auto()

class OtherInstanceState(Enum):
    AWAITING_BEGIN_BUFFER = auto()
    EMITTING = auto()
    AWAITING_CHUNKS = auto()
    REQUESTED_RESEND = auto()
    COMPLETE_BUFFER  = auto()

class InstanceData:
    def __init__(self, instance_count: int):
        def init_dict(default, count):
            return {i: default for i in range(count)}
        
        self._dependency_buffers = init_dict(bytes(), instance_count)  # instance_index -> joined buffer
        self._dependency_chunks = init_dict({}, instance_count)  # instance_index -> {chunk_id -> chunk_data}
        self._chunks_bitfield = init_dict(np.array([], dtype=bool), instance_count)  # instance_index -> bitfield tracking received/acked chunks
        self._states = init_dict(OtherInstanceState.AWAITING_BEGIN_BUFFER, instance_count)  # instance_index -> state
        self._received_all_chunks = init_dict(False, instance_count)  # instance_index -> received all flag
        self._expected_buffer_types = init_dict(0, instance_count)  # instance_index -> expected buffer type
        self._expected_chunk_ids = init_dict(None, instance_count)  # instance_index -> expected chunk ids

    def get_dependency_buffer(self, instance_index: int) -> bytes:
        return self._dependency_buffers[instance_index]
        
    def set_dependency_buffer(self, instance_index: int, buffer: bytes):
        self._dependency_buffers[instance_index] = buffer

    def get_dependency_chunks(self, instance_index: int) -> Dict[int, bytes]:
        return self._dependency_chunks[instance_index]
        
    def set_dependency_chunks(self, instance_index: int, chunks: Dict[int, bytes]):
        self._dependency_chunks[instance_index] = chunks

    def get_chunks_bitfield(self, instance_index: int) -> np.ndarray:
        return self._chunks_bitfield[instance_index]
        
    def set_chunks_bitfield(self, instance_index: int, bitfield: np.ndarray):
        self._chunks_bitfield[instance_index] = bitfield

    def get_state(self, instance_index: int) -> OtherInstanceState:
        return self._states[instance_index]
        
    def set_state(self, instance_index: int, state: OtherInstanceState):
        self._states[instance_index] = state

    def get_received_all_chunks(self, instance_index: int) -> bool:
        return self._received_all_chunks[instance_index]
        
    def set_received_all_chunks(self, instance_index: int, received: bool):
        self._received_all_chunks[instance_index] = received

    def get_expected_buffer_type(self, instance_index: int) -> int:
        return self._expected_buffer_types[instance_index]

    def set_expected_buffer_type(self, instance_index: int, buffer_type: int):
        self._expected_buffer_types[instance_index] = buffer_type

    def get_expected_chunk_ids(self, instance_index: int) -> List[int]:
        return self._expected_chunk_ids[instance_index]

    def set_expected_chunk_ids(self, instance_index: int, chunk_ids: List[int]):
        self._expected_chunk_ids[instance_index] = chunk_ids

class ExecutingStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):

        self._thread_lock = threading.Lock()
        self._current_distribution_instance_index: int = 0

        self._this_instance_dependency_byte_buffer = bytes
        self._this_instance_dependency_buffer_type: int = -1
        self._this_instance_dependency_chunks: Dict[int, bytes] = {}  # chunk_id -> chunk_data

        self._instance_data = InstanceData(EnvVars.get_instance_count())
        self._this_instance_state: ThisInstanceState = ThisInstanceState.EMITTING if self._current_distribution_instance_index == EnvVars.get_instance_index() else ThisInstanceState.AWAITING_SEND_BEGIN_BUFFER

        self._chunk_lock = threading.Lock()
        self._received_acks = set()  # Track which instances have ACKed
        self._last_chunk_check = 0
        self._recieved_buffer_begin: Dict[int, bool] = {}  # instance_index -> received begin flag
        
        super().__init__(instance,
                         ClusterState.EXECUTING,
                         ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN         |
                         ClusterMessageType.DISTRIBUTE_BUFFER_RESEND        |
                         ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT      |
                         ClusterMessageType.DISTRIBUTE_BUFFER_NEXT          |
                         ClusterMessageType.DISTRIBUTE_BUFFER_ACK)
        logger.debug("Initialized ExecutingStateHandler")

    def _buffer_progress(self, instance_index: int):
        chunks_field = self._instance_data.get_chunks_bitfield(instance_index)
        if len(chunks_field) == 0:
            raise ValueError("No chunks field available - cannot calculate buffer progress")
        total_chunks = np.sum(chunks_field)  # Count True flags
        expected_total = len(self._instance_data.get_expected_chunk_ids(instance_index)) if self._instance_data.get_expected_chunk_ids(instance_index) else 0
        return total_chunks, expected_total

    def _has_received_all_instance_resend_requests(self):
        current_instance = EnvVars.get_instance_index()
        
        for instance_id in range(EnvVars.get_instance_count()):
            # Skip checking our own instance
            if instance_id == current_instance:
                continue
                
            state = self._instance_data.get_state(instance_id)
            if state not in [OtherInstanceState.REQUESTED_RESEND,
                           OtherInstanceState.COMPLETE_BUFFER]:
                return False
                
        return True

    def determine_if_emitting(self):
        if self._current_distribution_instance_index == EnvVars.get_instance_index():
            self._this_instance_state = ThisInstanceState.EMITTING
        elif self._current_distribution_instance_index >= EnvVars.get_instance_count():
            self._this_instance_state = ThisInstanceState.ALL_DEPENDENCIES_RESOLVED
        else: self._this_instance_state = ThisInstanceState.AWAITING_SEND_BEGIN_BUFFER

    async def handle_sending_state(self):
        # Only send chunks if we have any to send and all instances have sent resend requests
        if not self._has_received_all_instance_resend_requests():
            return

        for instance_id in range(EnvVars.get_instance_count()):
            self._instance_data.set_state(instance_id, OtherInstanceState.AWAITING_CHUNKS)

        # Get all instances that have acked
        acked_instances = []
        for i in range(EnvVars.get_instance_count()):
            if len(self._instance_data.get_chunks_bitfield(i)) > 0:
                acked_instances.append(i)
                
        if not acked_instances:
            return

        # Create a combined bitfield of all acked chunks
        chunk_count = len(self._this_instance_dependency_chunks)
        all_instance_missing_chunks = np.zeros(chunk_count, dtype=np.bool_)
        
        # For each instance that has acked, OR their ack bitfield
        for instance_index in acked_instances:
            all_instance_missing_chunks |= ~self._instance_data.get_chunks_bitfield(instance_index)

        # Find indices where chunks haven't been acked
        missing_chunks = np.nonzero(all_instance_missing_chunks)[0]
        
        if missing_chunks.size > 0:
            logger.debug(f"Resending {len(missing_chunks)} chunks that weren't acked by all instances")
            buffer_flag = int(123456789).to_bytes(4, byteorder='big')
            
            for chunk_id in missing_chunks:
                chunk_data = self._this_instance_dependency_chunks[chunk_id]
                chunk_id_bytes = int(chunk_id).to_bytes(4, byteorder='big')
                self._emit_byte_chunk(buffer_flag, chunk_id_bytes, chunk_data)
        
        else: # All instances have acked all chunks, propagate NEXT flag.
            message = ClusterDistributeBufferResend()
            message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_NEXT
            message.header.require_ack = True
            await self._instance.cluster.udp_message_handler.send_and_wait(message)
            self._current_distribution_instance_index = self._current_distribution_instance_index + 1
            self.determine_if_emitting()

    async def handle_receiving_state(self):
        current_time = time.time()
        if current_time - self._last_chunk_check >= 0.1:  # 10ms
            self._last_chunk_check = current_time
            
            total_chunks, expected_total = self._buffer_progress(self._current_distribution_instance_index)
            logger.debug(f"Instance {EnvVars.get_instance_index()}: Have {total_chunks}/{expected_total} chunks")
            if expected_total > 0:
                if total_chunks < expected_total:
                    # Create expected bitfield of 1s up to expected_total
                    expected_bitfield = np.ones(expected_total, dtype=np.bool_)
                    
                    missing_bitfield = ~self._instance_data.get_chunks_bitfield(self._current_distribution_instance_index) & expected_bitfield
                    missing_bytes = np.packbits(missing_bitfield).tobytes()
                    message = ClusterDistributeBufferResend()
                    message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_RESEND
                    message.header.require_ack = True
                    message.instance_index = EnvVars.get_instance_index()
                    message.missing_chunk_ids = missing_bytes
                    await self._instance.cluster.udp_message_handler.send_and_wait(message, self._current_distribution_instance_index)

                elif not self._instance_data.get_received_all_chunks(self._current_distribution_instance_index):
                    message = ClusterDistributeBufferAck()
                    message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ACK
                    message.header.require_ack = True
                    message.instance_index = EnvVars.get_instance_index()
                    await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message, self._current_distribution_instance_index)

                    logger.info(f"All chunks received from instance {self._current_distribution_instance_index}, joining buffers")
                    chunks = self._instance_data.get_dependency_chunks(self._current_distribution_instance_index)
                    joined_buffer = b''.join(chunks.values())
                    self._instance_data.set_dependency_buffer(self._current_distribution_instance_index, joined_buffer)
                    self._instance_data.set_received_all_chunks(self._current_distribution_instance_index, True)

    async def handle_state(self, current_state: int) -> StateResult | None:
        with self._thread_lock:
            if self._this_instance_state == ThisInstanceState.EMITTING:
                await self.handle_sending_state()
            elif self._this_instance_state == ThisInstanceState.AWAITING_CHUNKS:
                await self.handle_receiving_state()

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        with self._thread_lock:
            if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN:
                distribute_buffer = ParseDict(incoming_message.message, ClusterDistributeBufferBegin())
                sender_instance = distribute_buffer.instance_index
                logger.debug(f"Received buffer begin message from instance {sender_instance}, expecting {distribute_buffer.chunk_count} chunks")
                self._instance_data.set_expected_buffer_type(sender_instance, distribute_buffer.buffer_type)
                self._instance_data.set_expected_chunk_ids(sender_instance, list(range(distribute_buffer.chunk_count)))
                self._instance_data.set_dependency_chunks(sender_instance, {})
                self._instance_data.set_chunks_bitfield(sender_instance, np.zeros(distribute_buffer.chunk_count, dtype=np.bool_))
                self._instance_data.set_state(sender_instance, OtherInstanceState.EMITTING)
                self._this_instance_state = ThisInstanceState.AWAITING_CHUNKS

            elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
                if self._instance_data.get_state(incoming_message.sender_instance_id) != OtherInstanceState.COMPLETE_BUFFER:
                    resend_msg = ParseDict(incoming_message.message, ClusterDistributeBufferResend())
                    missing_bits = np.unpackbits(np.frombuffer(resend_msg.missing_chunk_ids, dtype=np.uint8)).astype(bool)
                    expected_length = len(self._instance_data.get_expected_chunk_ids(EnvVars.get_instance_index()))
                    missing_bits = missing_bits[:expected_length]
                    logger.debug("Received resend request for %d chunks", len(np.nonzero(missing_bits)[0]))
                    self._instance_data.set_chunks_bitfield(incoming_message.sender_instance_id, ~missing_bits)
                    self._instance_data.set_state(incoming_message.sender_instance_id, OtherInstanceState.REQUESTED_RESEND)

            elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_NEXT:
                self._current_distribution_instance_index = self._current_distribution_instance_index + 1
                self.determine_if_emitting()

            elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
                logger.debug("Received buffer ACK")
                ack_msg = ParseDict(incoming_message.message, ClusterDistributeBufferAck())
                self._received_acks.add(ack_msg.instance_index)
                self._instance_data.set_state(ack_msg.instance_index, OtherInstanceState.COMPLETE_BUFFER)
                self._instance_data.set_chunks_bitfield(ack_msg.instance_index, np.ones(len(self._instance_data.get_chunks_bitfield(ack_msg.instance_index)), dtype=np.bool_))
        return None

    async def handle_buffer(self, current_state: int, byte_buffer, addr: str) -> StateResult | None:
        with self._thread_lock:
            buffer_view = memoryview(byte_buffer)
            chunk_id = int.from_bytes(buffer_view[4:8], byteorder='big')
            
            # Determine sender instance from addr
            sender_instance = None
            for i in range(EnvVars.get_instance_count()):
                if i == EnvVars.get_instance_index():
                    continue
                if addr == self._instance.cluster.instances[i].address:
                    sender_instance = i
                    break
                    
            if sender_instance is not None:
                chunks = self._instance_data.get_dependency_chunks(sender_instance)
                chunks[chunk_id] = buffer_view[HEADER_SIZE:].tobytes()
                self._instance_data.set_dependency_chunks(sender_instance, chunks)

                # Update bitfield to mark this chunk as received
                chunks_bitfield = self._instance_data.get_chunks_bitfield(sender_instance)
                chunks_bitfield[chunk_id] = True
                self._instance_data.set_chunks_bitfield(sender_instance, chunks_bitfield)

                # Check progress less frequently
                if chunk_id % 100 == 0 or (chunk_id == len(self._instance_data.get_expected_chunk_ids(sender_instance)) - 1):
                    total_chunks, expected_total = self._buffer_progress(sender_instance)
                    logger.debug(f"Received chunk {chunk_id} from instance {sender_instance}. Total: {total_chunks}/{expected_total}")

    def _emit_byte_chunk(self, buffer_flag: bytes, chunk_id_bytes: bytes, chunk_data: bytes, instance_id: int | None = None):
        chunk_with_id = buffer_flag + chunk_id_bytes + chunk_data
        self._instance.cluster.udp_buffer_handler.queue_byte_buffer(chunk_with_id, instance_id)

    async def _emit_all_sent_message(self):
        instance_index = EnvVars.get_instance_index()
        all_sent_msg = ClusterDistributeBufferAllSent()
        all_sent_msg.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT
        all_sent_msg.header.require_ack = True
        all_sent_msg.instance_index = instance_index
        await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(all_sent_msg)

    def _create_chunks(self, byte_buffer):
        chunk_size = UDP_MTU - HEADER_SIZE
        chunk_count = math.ceil(len(byte_buffer) / (UDP_MTU - HEADER_SIZE))
        chunk_ids = list(range(chunk_count))
        self._this_instance_dependency_chunks = {}

        buffer_view = memoryview(byte_buffer)
        # buffer_flag = int(123456789).to_bytes(4, byteorder='big')

        for i in range(chunk_count):
            start = i * chunk_size
            end = min(start + chunk_size, len(byte_buffer))
            chunk_data = buffer_view[start:end].tobytes()
            chunk_id = chunk_ids[i]
            self._this_instance_dependency_chunks[chunk_id] = chunk_data

        self._instance_data.set_expected_chunk_ids(EnvVars.get_instance_index(), chunk_ids)
        self._instance_data.set_chunks_bitfield(EnvVars.get_instance_index(), np.ones(len(self._this_instance_dependency_chunks), dtype=bool))

    async def _distribute_buffer(self, buffer_type: int, byte_buffer: bytes) -> list[bytes]:
        instance_index = EnvVars.get_instance_index()

        # Wait until it's our turn to distribute
        while self._this_instance_state != ThisInstanceState.EMITTING:
            logger.debug("Awaiting our turn to distribute. Instance index currently distributing: %d, our instance index: %d", self._current_distribution_instance_index, instance_index)
            await asyncio.sleep(0.5)
            
        self._this_instance_dependency_byte_buffer = byte_buffer
        self._this_instance_dependency_buffer_type = buffer_type
        instance_index = EnvVars.get_instance_index()
        byte_buffer_size = len(self._this_instance_dependency_byte_buffer)
        chunk_count = math.ceil(byte_buffer_size / (UDP_MTU - HEADER_SIZE))

        self._create_chunks(self._this_instance_dependency_byte_buffer)

        # Prepare distribution message
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

        logger.debug("Sending buffer begin message")
        await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message)

        while True:
            if self._this_instance_state == ThisInstanceState.ALL_DEPENDENCIES_RESOLVED:
                logger.info("Buffer distribution complete")
                instance_count = EnvVars.get_instance_count()
                buffers = []
                for i in range(instance_count):
                    if i == EnvVars.get_instance_index():
                        buffers.append(b'')  # Empty buffer for own index
                    else:
                        buffers.append(self._instance_data.get_dependency_buffer(i))
                return buffers
                    
            logger.debug("Waiting for buffers from all instances...")
            await asyncio.sleep(0.5)

    async def distribute_tensor(self, tensor: torch.Tensor) -> torch.Tensor:
        # TODO: Current implementation assumes all tensors have same shape
        # Should validate shapes match before combining

        logger.info("Distributing tensor of shape %s", tensor.shape)
        # Get original shape and convert tensor to bytes
        original_shape = tensor.shape
        byte_buffer = tensor.numpy().tobytes()

        # This should keep blocking until we get all buffers.
        buffers = await self._distribute_buffer(ClusterBufferType.TENSOR, byte_buffer)
        
        # Reconstruct tensor from received buffers
        instance_count = EnvVars.get_instance_count()
        instance_index = EnvVars.get_instance_index()
        
        # Insert tensor bytes at instance index
        buffers[instance_index] = byte_buffer
        
        combined_buffer = b''.join(buffers)
        array = np.frombuffer(combined_buffer, dtype=np.float32)
        
        # Reshape into batch tensor using original shape
        batch_shape = (instance_count,) + original_shape
        batch_tensor = torch.from_numpy(array).reshape(batch_shape)
        
        logger.info("Tensor distribution complete. Final shape: %s", batch_tensor.shape)
        return batch_tensor
