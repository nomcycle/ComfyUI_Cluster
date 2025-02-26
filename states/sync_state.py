from enum import Enum, auto
from typing import Callable, Dict, List, TYPE_CHECKING

import asyncio
import numpy as np
import torch
import threading
import random

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterDistributeBufferBegin, 
    ClusterDistributeBufferAck, ClusterBufferType, ClusterMessageHeader,
    ClusterDistributeBufferResend, ClusterDistributeBufferDescriptor,
)

from .state_handler import StateHandler
from .state_result import StateResult
from ..instance import ThisInstance
from ..env_vars import EnvVars

from ..queued import IncomingMessage, IncomingBuffer

from .sync_handlers.emitter import Emitter
from .sync_handlers.receiver import Receiver

from .sync_handlers.sync_handler import SyncHandler

from ..synced_random import SyncedRandom

_fanout_expected_msg_key: int = abs(SyncedRandom.get_random_int())
_gather_expected_msg_key: int = abs(SyncedRandom.get_random_int())

logger.debug(f"Fanout expected message key: {_fanout_expected_msg_key}")
logger.debug(f"Gather expected message key: {_gather_expected_msg_key}")

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

    def _clear_delegates(self):
        self._message_handler_callback = None
        self._buffer_handler_callback = None
        self._state_handler_callback = None
    
    def _register_delegates(self, handle_message_callback, handle_buffer_callback, tick_callback):
        self._message_handler_callback = handle_message_callback
        self._buffer_handler_callback = handle_buffer_callback
        self._state_handler_callback = tick_callback

    async def _begin_fanout_receiver(self) -> list[bytes]:
        completed_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        receiver: Receiver = Receiver(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            completed_buffer)

        self._register_delegates(receiver.handle_message, receiver.handle_buffer, receiver.tick)

        await receiver.begin()
        result = await completed_buffer

        self._clear_delegates()

        return result.get_buffer()

    async def _begin_buffer_broadcast(self, byte_buffer: bytes):
        all_instanced_received_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        emitter: Emitter = Emitter(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            all_instanced_received_buffer,
            byte_buffer)

        self._register_delegates(emitter.handle_message, None, emitter.tick)

        await emitter.begin()
        await all_instanced_received_buffer

        self._clear_delegates()

    async def _begin_fanout_emitter(self, tensor: torch.Tensor):

        for instance_id in range(EnvVars.get_instance_count()):

            # Extract single image from batch
            image_tensor = tensor[instance_id:instance_id+1]
            byte_buffer = image_tensor.numpy().tobytes()

            all_instanced_received_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
            emitter: Emitter = Emitter(
                self._instance.cluster.udp_message_handler,
                self._instance.cluster.udp_buffer_handler,
                asyncio.get_running_loop(),
                all_instanced_received_buffer,
                byte_buffer,
                to_instance_ids=instance_id)

            self._register_delegates(emitter.handle_message, None, emitter.tick)

            await emitter.begin()
            await all_instanced_received_buffer

            self._clear_delegates()

    async def _begin_gather_tensors(self, tensor: torch.Tensor) -> tuple[list[bytes],tuple[int]]:

        byte_buffer = tensor.numpy().tobytes()

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

        received_buffer_shapes: list[tuple[int]] = [None] * EnvVars.get_instance_count()
        received_buffer_shapes[EnvVars.get_instance_index()] = tensor.shape

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

                await self._send_buffer_descriptor(tensor, _gather_expected_msg_key)
                await emitter.begin()
                await all_instanced_received_buffer

                self._clear_delegates()

            else: # If we are currently receiving.

                self._register_delegates(
                    receivers[current_emitter_instance_id].handle_message,
                    receivers[current_emitter_instance_id].handle_buffer,
                    receivers[current_emitter_instance_id].tick)

                buffer_descriptor: ClusterDistributeBufferDescriptor = await self._receive_buffer_descriptor(_gather_expected_msg_key)
                await receivers[current_emitter_instance_id].begin()

                result = await on_instance_received_buffer[current_emitter_instance_id]
                buffer = result.get_buffer()
                if len(buffer) == 0:
                    raise Exception(f"Failed to receive buffer from instance {current_emitter_instance_id}")
                received_buffers[current_emitter_instance_id] = buffer
                received_buffer_shapes[current_emitter_instance_id] = tuple(buffer_descriptor.buffer_shape)

                self._clear_delegates()

        return received_buffers, received_buffer_shapes

    async def begin_tensor_broadcast(self, tensor: torch.Tensor):

        logger.info("Distributing tensor of shape %s", tensor.shape)
        byte_buffer = tensor.numpy().tobytes()

        await self._begin_buffer_broadcast(byte_buffer)
    
    async def _send_buffer_descriptor(self, tensor: torch.Tensor, expected_key: int):
        message = ClusterDistributeBufferDescriptor()
        message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_DESCRIPTOR
        message.header.require_ack = True
        message.buffer_shape.extend(list(tensor.shape))
        await self._instance.cluster.udp_message_handler.send_expected_message_thread_safe(message, expected_key) # Fixed typo in method name

    async def _receive_buffer_descriptor(self, expected_key: int) -> ClusterDistributeBufferDescriptor:
        result = await self._instance.cluster.udp_message_handler.await_expected_message_thread_safe(expected_key) # Fixed typo in method name
        if not result.success or not result.data:
            raise Exception("Failed to receive fanout tensor metadata")

        incoming_message: IncomingMessage = result.data
        buffer_descriptor = ParseDict(incoming_message.message, ClusterDistributeBufferDescriptor())

        if not buffer_descriptor.buffer_shape or len(buffer_descriptor.buffer_shape) == 0:
            raise Exception("Invalid buffer descriptor - missing shape information")

        logger.debug(f"Received buffer descriptor from instance {incoming_message.sender_instance_id} for tensor with shape: {buffer_descriptor.buffer_shape}")
        return buffer_descriptor

    async def begin_fanout_emitter(self, tensor: torch.Tensor):
        logger.info("Distributing tensor of shape %s", tensor.shape)

        await self._send_buffer_descriptor(tensor, _fanout_expected_msg_key)

        # Convert tensor to bytes and distribute
        byte_buffer = tensor.numpy().tobytes()
        await self._begin_buffer_broadcast(byte_buffer)

        self._exit_state = True
        return tensor

    async def begin_fanout_receiver(self) -> torch.Tensor:

        buffer_descriptor: ClusterDistributeBufferDescriptor = await self._receive_buffer_descriptor(_fanout_expected_msg_key)

        buffer = await self._begin_fanout_receiver() # Fixed incorrect method name
        array = np.frombuffer(buffer, dtype=np.float32)
        tensor = torch.from_numpy(array).reshape(tuple(buffer_descriptor.buffer_shape))
        
        logger.info("Received fanout tensor of shape %s", tensor.shape)
        
        self._exit_state = True
        return tensor

    async def begin_gathering_tensors(self, tensor: torch.Tensor) -> torch.Tensor:
        # TODO: Current implementation assumes all tensors have same shape
        # Should validate shapes match before combining

        logger.info("Syncing tensor of shape %s", tensor.shape)
        # Get original shape and convert tensor to bytes

        # This should keep blocking until we get all buffers.
        buffers, shapes = await self._begin_gather_tensors(tensor)
        
        # Convert each buffer into a tensor with its original shape
        tensors = []
        for buffer, shape in zip(buffers, shapes):
            array = np.frombuffer(buffer, dtype=np.float32)
            tensor = torch.from_numpy(array).reshape(shape)
            tensors.append(tensor)
            
        self._exit_state = True
        return tensors
