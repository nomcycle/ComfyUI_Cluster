from typing import Callable, Dict, List, TYPE_CHECKING

import asyncio
import numpy as np
import torch
import threading

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterDistributeBufferBegin, 
    ClusterDistributeBufferAck, ClusterBufferType, ClusterMessageHeader,
    ClusterDistributeBufferResend
)

from .state_handler import StateHandler
from .state_result import StateResult
from ..instance import ThisInstance
from ..env_vars import EnvVars

from ..queued import IncomingMessage, IncomingBuffer

from .sync_handlers.emitter import Emitter
from .sync_handlers.receiver import Receiver

from .sync_handlers.sync_handler import SyncHandler

class SyncStateHandler(StateHandler):
    def __init__(self, instance: ThisInstance):

        self._chunk_lock = threading.Lock()
        self._received_acks = set()  # Track which instances have ACKed
        self._recieved_buffer_begin: Dict[int, bool] = {}  # instance_index -> received begin flag

        self._message_handler_callback: Callable[[int, IncomingMessage], StateResult | None] = None
        self._buffer_handler_callback: Callable[[int, IncomingBuffer], StateResult | None] = None
        self._state_handler_callback: Callable[[], None] = None

        self._sync_handler: SyncHandler | None = None
        self._exit_state: bool = False

        super().__init__(instance,
                         ClusterState.EXECUTING,
                         ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN         |
                         ClusterMessageType.DISTRIBUTE_BUFFER_RESEND        |
                         ClusterMessageType.DISTRIBUTE_BUFFER_ACK)

        logger.debug("Initialized ExecutingStateHandler")

    async def handle_state(self, current_state: int) -> StateResult | None:
        if self._state_handler_callback and callable(self._state_handler_callback):
            await self._state_handler_callback()
        if self._exit_state:
            from .idle_state import IdleStateHandler
            return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))
        return None

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        if self._message_handler_callback is None or not callable(self._message_handler_callback):
            return None
        return await self._message_handler_callback(current_state, incoming_message)

    async def handle_buffer(self, current_state: int, incoming_buffer: IncomingBuffer) -> StateResult | None:
        if self._buffer_handler_callback is None or not callable(self._buffer_handler_callback):
            return None
        return await self._buffer_handler_callback(current_state, incoming_buffer)

    async def _fence_instances(self) -> bool:
        result = await self._instance.cluster.udp_message_handler.await_fence_thread_safe(69)
        if not result.success:
            raise Exception("Failed to fence instances")

    def _clear_delegates(self):
        self._message_handler_callback = None
        self._buffer_handler_callback = None
        self._state_handler_callback = None
    
    def _register_delegates(self, handle_message_callback, handle_buffer_callback, tick_callback):
        self._message_handler_callback = handle_message_callback
        self._buffer_handler_callback = handle_buffer_callback
        self._state_handler_callback = tick_callback

    async def _receive_buffer(self) -> list[bytes]:
        completed_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        receiver: Receiver = Receiver(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            completed_buffer)

        self._register_delegates(receiver.handle_message, receiver.handle_buffer, receiver.tick)

        await self._fence_instances()
        result = await completed_buffer

        self._clear_delegates()

        return result.buffer

    async def _fanout_buffer(self, byte_buffer: bytes):
        all_instanced_received_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        emitter: Emitter = Emitter(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            all_instanced_received_buffer,
            byte_buffer)

        self._register_delegates(emitter.handle_message, None, emitter.tick)

        await self._fence_instances()
        await emitter.begin()
        await all_instanced_received_buffer

        self._clear_delegates()

    async def _sync_buffers(self, byte_buffer: bytes) -> list[bytes]:

        all_instanced_received_buffer: asyncio.Future = asyncio.get_running_loop().create_future()

        emitter: Emitter = Emitter(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            all_instanced_received_buffer,
            byte_buffer)

        on_instance_received_buffer: List[asyncio.Future] = [None] * EnvVars.get_instance_count()
        receivers: List[Receiver] = [None] * EnvVars.get_instance_count()
        received_buffers: List[bytes] = [None] * EnvVars.get_instance_count()
        received_buffers[EnvVars.get_instance_index()] = byte_buffer

        for instance_index in range(EnvVars.get_instance_count()):
            on_instance_received_buffer[instance_index] = asyncio.get_running_loop().create_future()
            receivers[instance_index] = Receiver(
                self._instance.cluster.udp_message_handler,
                self._instance.cluster.udp_buffer_handler,
                asyncio.get_running_loop(),
                on_instance_received_buffer[instance_index])

        for current_emitter_instance_id in range(EnvVars.get_instance_count()):

            if current_emitter_instance_id == EnvVars.get_instance_index():
                self._register_delegates(emitter.handle_message, None, emitter.tick)

                await self._fence_instances()
                await emitter.begin()
                await all_instanced_received_buffer

                self._clear_delegates()

            else: # If we are currently receiving.

                self._register_delegates(
                    receivers[current_emitter_instance_id].handle_message,
                    receivers[current_emitter_instance_id].handle_buffer,
                    receivers[current_emitter_instance_id].tick)

                await self._fence_instances()
                result = await on_instance_received_buffer[current_emitter_instance_id]
                buffer = result.get_buffer()
                if len(buffer) == 0:
                    raise Exception(f"Failed to receive buffer from instance {current_emitter_instance_id}")
                received_buffers[current_emitter_instance_id] = buffer

                self._clear_delegates()

        return received_buffers

    async def fanout_tensor(self, tensor: torch.Tensor):

        logger.info("Distributing tensor of shape %s", tensor.shape)
        byte_buffer = tensor.numpy().tobytes()

        await self._fanout_buffer(byte_buffer)

    async def receive_tensor(self) -> torch.Tensor:
        return await self._receive_buffer()

    async def sync_tensors(self, tensor: torch.Tensor) -> torch.Tensor:
        # TODO: Current implementation assumes all tensors have same shape
        # Should validate shapes match before combining

        logger.info("Syncing tensor of shape %s", tensor.shape)
        # Get original shape and convert tensor to bytes
        original_shape = tensor.shape
        byte_buffer = tensor.numpy().tobytes()

        # This should keep blocking until we get all buffers.
        buffers = await self._sync_buffers(byte_buffer)
        
        # Reconstruct tensor from received buffers
        instance_count = EnvVars.get_instance_count()
        
        combined_buffer = b''.join(buffers)
        array = np.frombuffer(combined_buffer, dtype=np.float32)
        
        # Reshape into batch tensor using original shape
        batch_shape = (instance_count,) + original_shape
        batch_tensor = torch.from_numpy(array).reshape(batch_shape)
        
        logger.info("Tensor distribution complete. Final shape: %s", batch_tensor.shape)

        # self._this_instance_state = ThisInstanceState.DONE

        # result = await self._instance.cluster.udp_message_handler.request_state_thread_safe(ClusterState.IDLE)
        # if not result.success:
        #     return

        self._exit_state = True
        return batch_tensor
