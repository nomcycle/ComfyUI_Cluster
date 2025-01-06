
import asyncio
import random
import time
import numpy as np
import torch
import math
import threading
from typing import Dict, List, TYPE_CHECKING

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterDistributeBufferBegin, 
    ClusterDistributeBufferAck, ClusterBufferType, ClusterMessageHeader,
)

from .state_handler import StateHandler
from .state_result import StateResult
from ..instance import ThisInstance
from ..env_vars import EnvVars

class ExecutingStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        self._expected_buffer_type: int = 0
        self._expected_chunk_ids: List[int] = None
        self._received_chunks: Dict[int, Dict[int, bytes]] = None  # instance_index -> chunk_id -> chunk_data
        self._received_buffers_from_all_instances: bool = False
        self._buffers: Dict[int, bytes] = {}  # instance_index -> joined buffer
        self._chunk_lock = threading.Lock()
        super().__init__(instance, ClusterState.EXECUTING, ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN | ClusterMessageType.DISTRIBUTE_BUFFER_ACK)

    async def handle_state(self, current_state: int) -> StateResult | None:
        with self._chunk_lock:
            if self._received_buffers_from_all_instances:
                message = ClusterDistributeBufferAck()
                message.type = ClusterMessageType.DISTRIBUTE_BUFFER_ACK,
                message.instance_index = EnvVars.get_instance_index()
                message.header.require_ack = True
                await self._instance.cluster.udp.send_and_wait(message)
                return None

        await asyncio.sleep(0.1)
        return None

    def handle_message(self, current_state: int, msg_type: int, message, addr: str) -> StateResult | None:
        if msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN:
            distribute_buffer = ParseDict(message, ClusterDistributeBufferBegin())
            with self._chunk_lock:
                self._expected_buffer_type = distribute_buffer.buffer_type
                self._expected_chunk_ids = distribute_buffer.chunk_ids
                self._received_chunks = {}
        elif msg_type == ClusterMessageType.DISTRIBUTE_BUFFER_ACK:
            pass
        return None

    def handle_buffer(self, current_state: int, byte_buffer, addr: str) -> StateResult | None:
        with self._chunk_lock:
            if self._received_buffers_from_all_instances:
                raise Exception("Already received buffers from all instances")

            if not byte_buffer or len(byte_buffer) < 12:  # Need at least 12 bytes (4 for flag + 4 for instance index + 4 for chunk id)
                logger.error("Received invalid buffer")
                return None

            instance_index = int.from_bytes(byte_buffer[4:8], byteorder='big')  # Skip first 4 bytes (buffer flag)
            chunk_id = int.from_bytes(byte_buffer[8:12], byteorder='big')  # Skip first 8 bytes (4 buffer flag + 4 instance index)
            chunk_data = byte_buffer[12:]  # Skip first 12 bytes (4 buffer flag + 4 instance index + 4 chunk id)
        
            if not self._expected_chunk_ids or chunk_id not in self._expected_chunk_ids:
                logger.warning("Received unexpected chunk id %d", chunk_id)
                return None
                
            if instance_index not in self._received_chunks:
                self._received_chunks[instance_index] = {}
                
            self._received_chunks[instance_index][chunk_id] = chunk_data
            
            total_chunks = sum(map(len, self._received_chunks.values()))
            expected_total = len(self._expected_chunk_ids) * EnvVars.get_instance_count()
            if total_chunks == expected_total:
                for instance_index, instance_chunks in self._received_chunks.items():
                    ordered_chunks = [instance_chunks[chunk_id] for chunk_id in self._expected_chunk_ids]
                    self._buffers[instance_index] = b''.join(ordered_chunks)
                self._received_buffers_from_all_instances = True
        return None

    def generate_key(self) -> int:
        return random.randint(2**16 - 1, 2**32 - 1)

    async def distribute_buffer(self, buffer_type: int, byte_buffer: bytes) -> list[bytes]:
        byte_buffer_size = len(byte_buffer)
        HEADER_SIZE = 12  # 4 bytes each for buffer flag, chunk id, instance index
        UDP_MTU = 1460
        chunk_count = math.ceil((HEADER_SIZE + byte_buffer_size) / UDP_MTU)
        chunk_ids = [self.generate_key() for _ in range(chunk_count)]
        
        instance_index = EnvVars.get_instance_index()
        logger.info(f"Distributing buffer of size {byte_buffer_size} bytes in {chunk_count} chunks...")

        # Prepare distribution message
        message = ClusterDistributeBufferBegin(
            header=ClusterMessageHeader(
                type=ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN,
                require_ack=True
            ),
            instance_index=instance_index,
            buffer_type=buffer_type,
            buffer_byte_size=byte_buffer_size,
            chunk_count=chunk_count,
            chunk_ids=chunk_ids
        )

        # Send message and chunks
        common_chunk_byte_header = instance_index.to_bytes(4, byteorder='big')
        await self._instance.cluster.udp.send_and_wait_thread_safe(message)
        self._instance.cluster.udp.emit_bytes(chunk_count, chunk_ids, common_chunk_byte_header, byte_buffer)

        # Wait for buffers with timeout
        timeout = 30  # 30 second timeout
        start_time = time.time()
        
        while True:
            with self._chunk_lock:
                if self._received_buffers_from_all_instances:
                    logger.info("All buffers received successfully")
                    instance_count = EnvVars.get_instance_count()
                    return [self._buffers[i] for i in range(instance_count)]
                    
            if time.time() - start_time > timeout:
                raise TimeoutError("Timed out waiting for buffer distribution")
                
            logger.debug("Waiting for buffers from all instances...")
            await asyncio.sleep(0.1)

    async def distribute_tensor(self, tensor: torch.Tensor) -> torch.Tensor:
        # TODO: Current implementation assumes all tensors have same shape
        # Should validate shapes match before combining

        # Convert tensor to bytes and distribute
        byte_buffer = tensor.numpy().tobytes()
        buffers = await self.distribute_buffer(ClusterBufferType.TENSOR, byte_buffer)
        
        # Reconstruct tensor from received buffers
        instance_count = EnvVars.get_instance_count()
        combined_buffer = b''.join(buffers)
        array = np.frombuffer(combined_buffer, dtype=np.float32)
        return torch.from_numpy(array).reshape(instance_count, -1)