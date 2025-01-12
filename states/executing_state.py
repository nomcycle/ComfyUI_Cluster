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

class ExecutingStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        self._expected_buffer_type: int = 0
        self._expected_chunk_ids: List[int] = None

        self._sent_chunks: Dict[int, bytes] = {}  # chunk_id -> chunk_data

        self._received_chunks: Dict[int, bytes] = {}  # chunk_id -> chunk_data
        self._received_buffers_from_all_instances: bool = False

        self._received_buffers: Dict[int, bytes] = {}  # instance_index -> joined buffer
        self._current_byte_buffer = bytes
        self._current_buffer_type: int = -1

        self._chunk_lock = threading.Lock()
        self._missing_chunk_ids = []
        self._current_distribution_instance_index: int = 0
        self._received_acks = set()  # Track which instances have ACKed
        self._last_chunk_check = 0
        self._recieved_buffer_begin: bool = False
        
        super().__init__(instance,
                         ClusterState.EXECUTING,
                         ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN         |
                         ClusterMessageType.DISTRIBUTE_BUFFER_RESEND        |
                         ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT      |
                         ClusterMessageType.DISTRIBUTE_BUFFER_ACK)
        logger.debug("Initialized ExecutingStateHandler")

    def _buffer_progress(self):
        total_chunks = len(self._received_chunks)
        expected_total = len(self._expected_chunk_ids) if self._expected_chunk_ids else 0
        return total_chunks, expected_total

    async def handle_state(self, current_state: int) -> StateResult | None:
        # Check for missing chunks every 10ms
        if self._recieved_buffer_begin:
            current_time = time.time()
            if current_time - self._last_chunk_check >= 0.1:  # 10ms
                self._last_chunk_check = current_time
                
                total_chunks, expected_total = self._buffer_progress()
                logger.debug("Have %d/%d chunks", total_chunks, expected_total)
                if total_chunks < expected_total:
                    if self._received_chunks:
                        max_chunk_id = max(self._received_chunks.keys())
                        missing = []
                        # Check for missing chunks up to max_chunk_id
                        for i in range(max_chunk_id + 1):
                            if i not in self._received_chunks:
                                missing.append(i)
                        
                        # If no missing chunks below max_chunk_id, check if we're missing chunks above it
                        if not missing and max_chunk_id + 1 < expected_total:
                            missing.extend(range(max_chunk_id + 1, expected_total))
                            
                        if missing:
                            # logger.debug(f"Missing chunks below {max_chunk_id}: {missing}")
                            
                            message = ClusterDistributeBufferResend()
                            message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_RESEND
                            # message.header.require_ack = True
                            message.header.require_ack = False
                            message.instance_index = EnvVars.get_instance_index() + 1
                            message.missing_chunk_ids.extend(missing)
                            # await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message)
                            self._instance.cluster.udp_message_handler.send_no_wait(message)
                else:
                    logger.info("All chunks received successfully, joining buffers")
                    self._received_buffers[self._current_distribution_instance_index] = b''.join(self._received_chunks.values())
                    logger.debug("All buffers received, sending ACK")
                    self._current_distribution_instance_index = self._current_distribution_instance_index + 1
                    
                    message = ClusterDistributeBufferAck()
                    message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ACK
                    message.header.require_ack = True
                    message.instance_index = EnvVars.get_instance_index() + 1
                    await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message)

                    if self._current_distribution_instance_index == EnvVars.get_instance_count():
                        logger.debug("Received buffers from all instances")
                        self._received_buffers_from_all_instances = True

        # await asyncio.sleep(0.001)  # Small sleep to prevent busy loop
        return None

    async def _handle_all_sent(self):
        total_chunks, expected_total = self._buffer_progress()
        logger.debug("All chunks sent signal received. Have %d/%d chunks", total_chunks, expected_total)
        if total_chunks == expected_total:
            logger.info("All chunks received successfully, joining buffers")
            self._received_buffers[self._current_distribution_instance_index] = b''.join(self._received_chunks.values())
            logger.debug("All buffers received, sending ACK")
            self._current_distribution_instance_index = self._current_distribution_instance_index + 1
            
            message = ClusterDistributeBufferAck()
            message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ACK
            message.header.require_ack = True
            message.instance_index = EnvVars.get_instance_index() + 1
            await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message)

            if self._current_distribution_instance_index == EnvVars.get_instance_count():
                logger.debug("Received buffers from all instances")
                self._received_buffers_from_all_instances = True

        else:
            # Calculate chunk size and max bytes we can request
            MTU = 1472  # Standard MTU size
            HEADER_SIZE = 8  # Buffer flag (4) + chunk ID (4)
            CHUNK_SIZE = MTU - HEADER_SIZE
            MAX_REQUEST_BYTES = 4 * 1024 * 1024  # 4MB in bytes

            # Find missing chunk IDs up to 16MB total size
            self._missing_chunk_ids = []
            current_bytes = 0
            
            for chunk_id in range(0, len(self._expected_chunk_ids)):
                if chunk_id not in self._received_chunks:
                    if current_bytes + CHUNK_SIZE > MAX_REQUEST_BYTES:
                        break
                    self._missing_chunk_ids.append(chunk_id)
                    current_bytes += CHUNK_SIZE

            message = ClusterDistributeBufferResend()
            message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_RESEND
            message.header.require_ack = True
            # message.header.require_ack = False
            message.instance_index = EnvVars.get_instance_index() + 1
            message.missing_chunk_ids.extend(self._missing_chunk_ids)
            await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message)
            # self._instance.cluster.udp_message_handler.send_no_wait(message)
            
            logger.warning("Missing %d chunks, requesting resend", len(self._missing_chunk_ids))

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN:
            distribute_buffer = ParseDict(incoming_message.message, ClusterDistributeBufferBegin())
            logger.debug("Received buffer begin message, expecting %d chunks", distribute_buffer.chunk_count)
            with self._chunk_lock:
                self._expected_buffer_type = distribute_buffer.buffer_type
                self._expected_chunk_ids = list(range(distribute_buffer.chunk_count))
                self._received_chunks = {}
                self._recieved_buffer_begin = True

        # elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT:
        #     if not self._instance.buffer_queue_empty():
        #         self._pending_all_sent = True
        #         return None
        #     await self._handle_all_sent()

        elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_RESEND:
            resend_msg = ParseDict(incoming_message.message, ClusterDistributeBufferResend())
            logger.debug("Received resend request for %d chunks", len(resend_msg.missing_chunk_ids))
            
            # Resend requested chunks
            buffer_flag = int(123456789).to_bytes(4, byteorder='big')

            for i, chunk_id in enumerate(resend_msg.missing_chunk_ids):
                chunk_id_bytes = chunk_id.to_bytes(4, byteorder='big')
                chunk_data = self._sent_chunks[chunk_id]
                self._emit_byte_chunk(buffer_flag, chunk_id_bytes, chunk_data, incoming_message.sender_addr)
                if (i + 1) % 10 == 0:
                    logger.debug(f"Resending chunk {chunk_id} ({i+1}/{len(resend_msg.missing_chunk_ids)})")

            # await self._emit_all_sent_message()
            
        elif incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
            logger.debug("Received buffer ACK")
            ack_msg = ParseDict(incoming_message.message, ClusterDistributeBufferAck())
            self._received_acks.add(ack_msg.instance_index)
            
            self._current_distribution_instance_index = self._current_distribution_instance_index + 1
            if self._current_distribution_instance_index == EnvVars.get_instance_count():
                logger.debug("Received ACKs from all instances, transitioning to IDLE state")
                self._received_buffers_from_all_instances = True
                from .idle_state import IdleStateHandler
                return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))
            return None
        return None

    async def handle_buffer(self, current_state: int, byte_buffer, addr: str) -> StateResult | None:
        buffer_view = memoryview(byte_buffer)
        chunk_id = int.from_bytes(buffer_view[4:8], byteorder='big')
        self._received_chunks[chunk_id] = buffer_view[HEADER_SIZE:].tobytes()
        
        # Check progress less frequently
        if chunk_id % 100 == 0:
            total_chunks, expected_total = self._buffer_progress() 
            logger.debug(f"Received chunk {chunk_id}. Total: {total_chunks}/{expected_total}") 

    def _generate_key(self) -> int:
        return random.randint(2**16 - 1, 2**32 - 1)

    def _emit_byte_chunk(self, buffer_flag: bytes, chunk_id_bytes: bytes, chunk_data: bytes, addr: str = None):
        chunk_with_id = buffer_flag + chunk_id_bytes + chunk_data
        self._instance.cluster.udp_buffer_handler.queue_byte_buffer(chunk_with_id, addr)

    async def _emit_all_sent_message(self):
        instance_index = EnvVars.get_instance_index()
        all_sent_msg = ClusterDistributeBufferAllSent()
        all_sent_msg.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ALL_SENT
        all_sent_msg.header.require_ack = True
        all_sent_msg.instance_index = instance_index
        await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(all_sent_msg)

    def _emite_byte_buffer(self, chunk_count: int, chunk_ids: [int], byte_buffer, addr: str = None):
        logger.debug("Emitting %d chunks totalling in size: %d", chunk_count, len(byte_buffer))

        chunk_size = UDP_MTU - HEADER_SIZE

        chunk_id_bytes = [id.to_bytes(4, byteorder='big') for id in chunk_ids]
        buffer_view = memoryview(byte_buffer)
        buffer_flag = int(123456789).to_bytes(4, byteorder='big')
        
        self._sent_chunks = {}
        for i in range(chunk_count):
            start = i * chunk_size
            end = min(start + chunk_size, len(byte_buffer))
            chunk_data = buffer_view[start:end].tobytes()
            self._emit_byte_chunk(buffer_flag, chunk_id_bytes[i], chunk_data, addr)
            chunk_id = chunk_ids[i]
            self._sent_chunks[chunk_id] = chunk_data
            if (i + 1) % 1000 == 0:
                logger.debug(f"Progress: {i+1}/{chunk_count} chunks emitted (chunk id: {chunk_id})")

        logger.debug("Finished emitting buffer.")

    def _send_chunks(self, chunk_count, chunk_ids, byte_buffer):
        logger.debug("Emitting buffer chunks")
        self._emite_byte_buffer(chunk_count, chunk_ids, byte_buffer)

    async def _distribute_buffer(self, buffer_type: int, byte_buffer: bytes) -> list[bytes]:
        instance_index = EnvVars.get_instance_index()
        
        # Wait until it's our turn to distribute
        while self._current_distribution_instance_index < instance_index:
            logger.debug("Awaiting our turn to distribute. Instance index currently distributing: %d, our instance index: %d", self._current_distribution_instance_index, instance_index)
            await asyncio.sleep(0.5)
            
        self._current_byte_buffer = byte_buffer
        self._current_buffer_type = buffer_type
        instance_index = EnvVars.get_instance_index()
        byte_buffer_size = len(self._current_byte_buffer)
        chunk_count = math.ceil(byte_buffer_size / (UDP_MTU - HEADER_SIZE))

        # Prepare distribution message
        message = ClusterDistributeBufferBegin(
            header=ClusterMessageHeader(
                type=ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN,
                require_ack=True
            ),
            instance_index=instance_index,
            buffer_type=self._current_buffer_type,
            buffer_byte_size=byte_buffer_size,
            chunk_count=chunk_count
        )

        logger.debug("Sending buffer begin message")
        await self._instance.cluster.udp_message_handler.send_and_wait_thread_safe(message)
        self._send_chunks(chunk_count, list(range(chunk_count)), self._current_byte_buffer)
        # await self._emit_all_sent_message()

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
