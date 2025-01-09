from typing import Dict, List, TYPE_CHECKING

import asyncio
import random
import numpy as np
import torch
import math
import threading
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

# Constants
HEADER_SIZE = 12  # 4 bytes each for buffer flag, chunk id, instance index
UDP_MTU = 1460

class ExecutingSubState(Enum):
    RECEIVING = auto()
    SENDING = auto()

class ReceiveState(Enum):
    RECEIVING = auto()
    REQUESTING_RESEND = auto()
    WAITING_ALL_ACK = auto()

class SendState(Enum):
    PREAMBLE = auto()
    SENDING = auto()

class ExecutingStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        self._expected_buffer_type: int = 0
        self._expected_chunk_ids: List[int] = None

        self._sent_chunks: Dict[int, bytes] = {}  # chunk_id -> chunk_data

        self._received_chunks: Dict[int, Dict[int, bytes]] = {}  # instance_index -> chunk_id -> chunk_data
        self._received_buffers_from_all_instances: bool = False

        self._received_buffers: Dict[int, bytes] = {}  # instance_index -> joined buffer

        self._chunk_lock = threading.Lock()
        self._substate = ExecutingSubState.RECEIVING
        self._receive_state = ReceiveState.RECEIVING
        self._send_state = SendState.PREAMBLE
        self._missing_chunk_ids = []
        self._current_distribution_instance_index = 0
        self._received_acks = set()  # Track which instances have ACKed
        super().__init__(instance, ClusterState.EXECUTING, ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN | ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT | ClusterMessageType.DISTRIBUTE_BUFFER_RESEND | ClusterMessageType.DISTRIBUTE_BUFFER_ACK)
        logger.debug("Initialized ExecutingStateHandler")

    async def handle_state(self, current_state: int) -> StateResult | None:
        if self._substate == ExecutingSubState.RECEIVING:
            if self._receive_state == ReceiveState.RECEIVING:
                if self._received_buffers_from_all_instances:
                    logger.debug("All buffers received, sending ACK")
                    message = ClusterDistributeBufferAck()
                    message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ACK
                    message.header.require_ack = True
                    message.instance_index = EnvVars.get_instance_index() + 1
                    await self._instance.cluster.udp.send_and_wait(message)
                    self._receive_state = ReceiveState.WAITING_ALL_ACK
                    return

            elif self._receive_state == ReceiveState.REQUESTING_RESEND:
                resend_msg = ClusterDistributeBufferResend()
                resend_msg.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_RESEND
                resend_msg.header.require_ack = True
                resend_msg.instance_index = EnvVars.get_instance_index()
                resend_msg.missing_chunk_ids.extend(self._missing_chunk_ids)
                await self._instance.cluster.udp.send_and_wait_thread_safe(resend_msg)
                self._receive_state = ReceiveState.RECEIVING
                return

        elif self._substate == ExecutingSubState.SENDING:
            if self._send_state == SendState.PREAMBLE:
                instance_index = EnvVars.get_instance_index()
                byte_buffer_size = len(self._current_byte_buffer)
                chunk_count = math.ceil(byte_buffer_size / (UDP_MTU - HEADER_SIZE))
                chunk_ids = [self._generate_key() for _ in range(chunk_count)]

                # Prepare distribution message
                message = ClusterDistributeBufferBegin(
                    header=ClusterMessageHeader(
                        type=ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN,
                        require_ack=True
                    ),
                    instance_index=instance_index,
                    buffer_type=self._current_buffer_type,
                    buffer_byte_size=byte_buffer_size,
                    chunk_count=chunk_count,
                    chunk_ids=chunk_ids
                )

                logger.debug("Sending buffer begin message")
                await self._instance.cluster.udp.send_and_wait_thread_safe(message)
                self._send_state = SendState.SENDING
                self._send_chunks(chunk_count, chunk_ids, self._current_byte_buffer)
                return

            elif self._send_state == SendState.SENDING:
                instance_index = EnvVars.get_instance_index()
                all_sent_msg = ClusterDistributeBufferAllSent()
                all_sent_msg.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT
                all_sent_msg.header.require_ack = True
                all_sent_msg.instance_index = instance_index
                await self._instance.cluster.udp.send_and_wait_thread_safe(all_sent_msg)
                return
            
        await asyncio.sleep(0.1)
        return None

    def handle_message(self, current_state: int, msg_type: int, message, addr: str) -> StateResult | None:
        if msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN:
            distribute_buffer = ParseDict(message, ClusterDistributeBufferBegin())
            logger.debug(f"Received buffer begin message, expecting {len(distribute_buffer.chunk_ids)} chunks")
            with self._chunk_lock:
                self._current_distribution_instance_index = distribute_buffer.instance_index
                self._expected_buffer_type = distribute_buffer.buffer_type
                self._expected_chunk_ids = distribute_buffer.chunk_ids
                self._received_chunks = {}
                self._substate = ExecutingSubState.RECEIVING
                self._receive_state = ReceiveState.RECEIVING

        elif msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT:
            total_chunks, expected_total = self._buffer_progress()
            logger.debug(f"All chunks sent signal received. Have {total_chunks}/{expected_total} chunks")
            if total_chunks == expected_total:
                logger.info("All chunks received successfully, joining buffers")
                for instance_index, instance_chunks in self._received_chunks.items():
                    ordered_chunks = [instance_chunks[chunk_id] for chunk_id in self._expected_chunk_ids]
                    self._received_buffers[instance_index] = b''.join(ordered_chunks)
                self._current_distribution_instance_index = self._current_distribution_instance_index + 1
                self._received_buffers_from_all_instances = True
            else:
                # Find missing chunk IDs
                self._missing_chunk_ids = []
                for chunk_id in self._expected_chunk_ids:
                    for instance_chunks in self._received_chunks.values():
                        if chunk_id not in instance_chunks:
                            self._missing_chunk_ids.append(chunk_id)
                
                logger.warning(f"Missing {len(self._missing_chunk_ids)} chunks, requesting resend")
                self._receive_state = ReceiveState.REQUESTING_RESEND

        elif msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
            if self._substate != ExecutingSubState.SENDING:
                return
            resend_msg = ParseDict(message, ClusterDistributeBufferResend())
            logger.debug(f"Received resend request for {len(resend_msg.missing_chunk_ids)} chunks")
            
            # Resend requested chunks
            instance_index = EnvVars.get_instance_index()
            common_chunk_byte_header = instance_index.to_bytes(4, byteorder='big')
            buffer_flag = int(123456789).to_bytes(4, byteorder='big')

            for i, chunk_id in enumerate(resend_msg.missing_chunk_ids):
                chunk_id_bytes = chunk_id.to_bytes(4, byteorder='big')
                chunk_data = self._sent_chunks[chunk_id]
                self._emit_byte_chunk(buffer_flag, chunk_id_bytes, common_chunk_byte_header, chunk_data, addr)
            
        elif msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
            logger.debug("Received buffer ACK")
            ack_msg = ParseDict(message, ClusterDistributeBufferAck())
            self._received_acks.add(ack_msg.instance_index)
            
            if self._received_buffers_from_all_instances and len(self._received_acks) == EnvVars.get_instance_count() - 1:
                logger.debug("Received ACKs from all instances, transitioning to IDLE state")
                from .idle_state import IdleStateHandler
                return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))
            return None
        return None

    def _buffer_progress(self):
        total_chunks = sum(map(len, self._received_chunks.values()))
        expected_total = len(self._expected_chunk_ids) * (EnvVars.get_instance_count() - 1)
        return total_chunks, expected_total

    def handle_buffer(self, current_state: int, byte_buffer, addr: str) -> StateResult | None:
        if self._received_buffers_from_all_instances:
            logger.error("Received buffer after all buffers already received")
            return
            # raise Exception("Already received buffers from all instances")

        if not byte_buffer or len(byte_buffer) < 12:  # Need at least 12 bytes (4 for flag + 4 for instance index + 4 for chunk id)
            logger.error("Received invalid buffer - too short")
            return None

        chunk_id = int.from_bytes(byte_buffer[4:8], byteorder='big') # Skip first 4 bytes (buffer flag)
        instance_index = int.from_bytes(byte_buffer[8:12], byteorder='big') # Skip first 8 bytes (4 buffer flag + 4 instance index)
        chunk_data = byte_buffer[12:]  # Skip first 12 bytes (4 buffer flag + 4 instance index + 4 chunk id)
    
        if not self._expected_chunk_ids or chunk_id not in self._expected_chunk_ids:
            logger.warning(f"Received unexpected chunk id {chunk_id}")
            return None
            
        if instance_index not in self._received_chunks:
            self._received_chunks[instance_index] = {}
            
        self._received_chunks[instance_index][chunk_id] = chunk_data
        
        total_chunks, expected_total = self._buffer_progress()
        # logger.debug(f"Received chunk {chunk_id} from instance {instance_index}. Total: {total_chunks}/{expected_total}")

    def _generate_key(self) -> int:
        return random.randint(2**16 - 1, 2**32 - 1)

    def _emit_byte_chunk(self, buffer_flag: bytes, chunk_id_bytes: bytes, common_chunk_byte_header: bytes, chunk_data: bytes, addr: str = None):
        chunk_with_id = buffer_flag + chunk_id_bytes + common_chunk_byte_header + chunk_data
        self._instance.cluster.udp.queue_byte_buffer(chunk_with_id, addr)

    def _emite_byte_buffer(self, chunk_count: int, chunk_ids: [int], common_chunk_byte_header: [], byte_buffer, addr: str = None):
        logger.debug(f"Emitting {chunk_count} chunks totalling in size: {len(byte_buffer)}")

        chunk_size = UDP_MTU - HEADER_SIZE

        chunk_id_bytes = [id.to_bytes(4, byteorder='big') for id in chunk_ids]
        buffer_view = memoryview(byte_buffer)
        buffer_flag = int(123456789).to_bytes(4, byteorder='big')
        
        self._sent_chunks = {}
        for i in range(chunk_count):
            start = i * chunk_size
            end = min(start + chunk_size, len(byte_buffer))
            chunk_data = buffer_view[start:end].tobytes()
            self._emit_byte_chunk(buffer_flag, chunk_id_bytes[i], common_chunk_byte_header, chunk_data, addr)
            chunk_id = chunk_ids[i]
            self._sent_chunks[chunk_id] = chunk_data
            # logger.debug(f"Progress: {i+1}/{chunk_count} chunks emitted (chunk id: {chunk_id})")

        logger.debug(f"Finished emitting buffer.")

    def _send_chunks(self, chunk_count, chunk_ids, byte_buffer):
        common_chunk_byte_header = EnvVars.get_instance_index().to_bytes(4, byteorder='big')
        logger.debug("Emitting buffer chunks")
        self._emite_byte_buffer(chunk_count, chunk_ids, common_chunk_byte_header, byte_buffer)

    async def _distribute_buffer(self, buffer_type: int, byte_buffer: bytes) -> list[bytes]:
        instance_index = EnvVars.get_instance_index()
        
        # Wait until it's our turn to distribute
        while self._current_distribution_instance_index < instance_index:
            logger.debug(f"Awaiting our turn to distribute. Instance index currently distributing: {self._current_distribution_instance_index}, our instance index: {instance_index}")
            await asyncio.sleep(0.5)
            
        self._current_byte_buffer = byte_buffer
        self._current_buffer_type = buffer_type
        self._substate = ExecutingSubState.SENDING
        self._send_state = SendState.PREAMBLE

        # Wait for buffers with timeout
        # timeout = 30  # 30 second timeout
        # start_time = time.time()
        
        while True:
            if self._received_buffers_from_all_instances:
                logger.info("Buffer distribution complete")
                instance_count = EnvVars.get_instance_count()
                buffers = []
                for i in range(instance_count):
                    if i == EnvVars.get_instance_index():
                        buffers.append(b'')  # Empty buffer for own index
                    else:
                        buffers.append(self._received_buffers[i])
                return buffers
                    
            # if time.time() - start_time > timeout:
            #     logger.error("Buffer distribution timed out")
            #     raise TimeoutError("Timed out waiting for buffer distribution")
                
            logger.debug("Waiting for buffers from all instances...")
            await asyncio.sleep(0.5)

    async def distribute_tensor(self, tensor: torch.Tensor) -> torch.Tensor:
        # TODO: Current implementation assumes all tensors have same shape
        # Should validate shapes match before combining

        logger.info(f"Distributing tensor of shape {tensor.shape}")
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
        
        logger.info(f"Tensor distribution complete. Final shape: {batch_tensor.shape}")
        return batch_tensor
