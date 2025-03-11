from datetime import datetime
from enum import Enum, auto
import json
from typing import Callable, Dict, List, TYPE_CHECKING

import asyncio
import numpy as np
import requests
import torch
import threading
import random

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterDistributeBufferBegin, 
    ClusterDistributeBufferAck, ClusterBufferType, ClusterMessageHeader,
    ClusterDistributeBufferResend, ClusterDistributeBufferDescriptor,
    ClusterDistributePrompt
)

from .state_handler import StateHandler
from .state_result import StateResult
from ..instance import ThisInstance
from ..env_vars import EnvVars

from ..queued import IncomingMessage, IncomingBuffer

from .sync_handlers.emitter import Emitter
from .sync_handlers.receiver import Receiver

from .sync_handlers.sync_handler import SyncHandler

from ..expected_msg import FANOUT_EXPECTED_MSG_KEY, GATHER_EXPECTED_MSG_KEY

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
                         ClusterMessageType.DISTRIBUTE_PROMPT               |
                         ClusterMessageType.DISTRIBUTE_BUFFER_BEGIN         |
                         ClusterMessageType.DISTRIBUTE_BUFFER_RESEND        |
                         ClusterMessageType.DISTRIBUTE_BUFFER_ACK)

        logger.debug("Initialized ExecutingStateHandler")

    async def handle_state(self, current_state: int) -> StateResult | None:
        if self._state_handler_callback and callable(self._state_handler_callback):
            await self._state_handler_callback()
        # if self._exit_state:
        #     from .idle_state import IdleStateHandler
        #     return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))
        return None

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_PROMPT:
            distribute_prompt = ParseDict(incoming_message.message, ClusterDistributePrompt())

            prompt_json = json.loads(distribute_prompt.prompt)
            json_data = {
                'prompt': prompt_json['output'],
                'extra_data': { 'extra_pnginfo': prompt_json['workflow'] },
                    "client_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
                    # "output_node_ids": self._flush_output_node_cache(prompt_json)
                }

            url = f"http://localhost:{EnvVars.get_comfy_port()}/prompt"
            try:
                response = requests.post(url, json=json_data)
                response.raise_for_status()
                logger.info("Successfully posted prompt to local ComfyUI instance")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error posting prompt: {str(e)}")

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

    async def _begin_buffer_broadcast(self, tensor: torch.tensor):
        all_instanced_received_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._send_buffer_descriptor(tensor, FANOUT_EXPECTED_MSG_KEY)
        byte_buffer = tensor.numpy().tobytes()
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
        return tensor
    
    def split_batch(self, tensor: torch.Tensor, instance_id: int) -> tuple[int, int]:
        batch_size = tensor.shape[0]
        instance_count = EnvVars.get_instance_count()
        base_per_instance = batch_size // instance_count
        remainder = batch_size % instance_count
        
        start_idx = instance_id * base_per_instance + min(instance_id, remainder)
        # If this instance gets an extra item from remainder
        extra = 1 if instance_id < remainder else 0
        end_idx = start_idx + base_per_instance + extra

        # Log batch splitting information
        logger.info(
            f"Instance {instance_id}: Splitting batch of size {batch_size} into {instance_count} parts. "
            f"Assigned slice [{start_idx}:{end_idx}] (size: {end_idx-start_idx})"
            f"{' with extra item from remainder' if extra else ''}"
        )
        
        return start_idx, end_idx

    async def _begin_fanout_emitter(self, tensor: torch.Tensor):
        # Calculate indices for current instance
        instance_id = EnvVars.get_instance_index()
        start_idx, end_idx = self.split_batch(tensor, instance_id)
        
        # Extract the tensor slice for the current instance
        output = tensor[start_idx:end_idx]

        for instance_id in range(EnvVars.get_instance_count()):
            if instance_id == EnvVars.get_instance_index():
                continue

            # Calculate indices for target instance
            start_idx, end_idx = self.split_batch(tensor, instance_id)
            
            # Extract the appropriate slice for this instance
            image_tensor = tensor[start_idx:end_idx]
            await self._send_buffer_descriptor(image_tensor, FANOUT_EXPECTED_MSG_KEY)
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

        return output

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

                await self._send_buffer_descriptor(tensor, GATHER_EXPECTED_MSG_KEY)
                await emitter.begin()
                await all_instanced_received_buffer

                self._clear_delegates()

            else: # If we are currently receiving.

                self._register_delegates(
                    receivers[current_emitter_instance_id].handle_message,
                    receivers[current_emitter_instance_id].handle_buffer,
                    receivers[current_emitter_instance_id].tick)

                buffer_descriptor: ClusterDistributeBufferDescriptor = await self._receive_buffer_descriptor(GATHER_EXPECTED_MSG_KEY)
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
        return await self._begin_buffer_broadcast(tensor)
    
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
        output = await self._begin_fanout_emitter(tensor)
        self._exit_state = True
        return output

    async def begin_fanout_receiver(self) -> torch.Tensor:

        buffer_descriptor: ClusterDistributeBufferDescriptor = await self._receive_buffer_descriptor(FANOUT_EXPECTED_MSG_KEY)

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
            if len(tensor.shape) < 4:  # Check if batch dimension is missing
                tensor = tensor.unsqueeze(0)  # Add batch dimension of 1
            tensors.append(tensor)
            
        self._exit_state = True
        return tensors
