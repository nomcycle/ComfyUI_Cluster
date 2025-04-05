"""
Sync state module - Handles tensor synchronization between cluster instances.
"""
import asyncio
import json
import threading
import traceback
from typing import Callable, Dict, List, Optional, Any, Tuple, Union

import numpy as np
import torch

from ..log import logger
from ..env_vars import EnvVars
from ..instance import ThisInstance
from ..udp.queued import IncomingMessage, IncomingBuffer
from ..udp.expected_msg import (
    FANIN_EXPECTED_MSG_KEY,
    FANOUT_EXPECTED_MSG_KEY,
    GATHER_EXPECTED_MSG_KEY,
    BEGIN_BUFFER_EXPECTED_MSG_KEY,
)

from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterMessageType,
    ClusterDistributeBufferDescriptor,
    ClusterDistributePrompt,
)

from .sync_handlers.emitter import Emitter
from .sync_handlers.receiver import Receiver, CompletedBufferEvent


class SyncStateHandler:
    """
    Handles tensor synchronization between cluster instances.
    
    This class provides methods for different tensor operations:
    - Broadcast: Send tensor to all instances
    - Fan-in: Gather tensors from followers to leader
    - Fan-out: Distribute slices of a tensor to followers
    - Gather: Collect tensors from all instances
    """
    
    def __init__(self, instance: ThisInstance):
        """
        Initialize the sync state handler.
        
        Args:
            instance: The instance this handler is attached to
        """
        self._instance = instance
        self._thread_lock = threading.Lock()
        
        # Callback references (used by emitters and receivers)
        self._message_handler_callback = None
        self._buffer_handler_callback = None
        self._state_handler_callback = None

    def _register_delegates(
        self, 
        handle_message_callback: Callable, 
        handle_buffer_callback: Optional[Callable],
        tick_callback: Callable
    ) -> None:
        """
        Register callback delegates for message and buffer handling.
        
        Args:
            handle_message_callback: Callback for handling messages
            handle_buffer_callback: Callback for handling buffers
            tick_callback: Callback for periodic processing
        """
        # Register with the instance
        self._instance._register_sync_callbacks(
            handle_message_callback,
            handle_buffer_callback,
            tick_callback
        )
        
        # Store locally for reference
        self._message_handler_callback = handle_message_callback
        self._buffer_handler_callback = handle_buffer_callback
        self._state_handler_callback = tick_callback

    def _clear_delegates(self) -> None:
        """Clear all callback delegates."""
        # Clear in the instance
        self._instance._clear_sync_callbacks()
        
        # Clear local references
        self._message_handler_callback = None
        self._buffer_handler_callback = None
        self._state_handler_callback = None

    async def _receive(self) -> bytes:
        """
        Receive a buffer from another instance.
        
        Returns:
            The received buffer as bytes
        """
        # Create a future for the completed buffer
        completed_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        
        # Create a receiver
        receiver = Receiver(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            completed_buffer,
        )

        # Register the receiver's callbacks
        self._register_delegates(
            receiver.handle_message, receiver.handle_buffer, receiver.tick
        )

        # Start the receiver and wait for completion
        await receiver.begin()
        result = await completed_buffer

        # Clear the callbacks
        self._clear_delegates()

        # Return the received buffer
        return result.get_buffer()

    async def _begin_buffer_sender(
        self,
        tensor: torch.Tensor,
        expected_msg_key: int,
        to_instance_ids: Optional[Union[int, List[int]]] = None,
    ) -> torch.Tensor:
        """
        Begin sending a buffer to other instances.
        
        Args:
            tensor: The tensor to send
            expected_msg_key: The message key for the operation
            to_instance_ids: The IDs of instances to send to (None for all)
            
        Returns:
            The original tensor
        """
        # Create a future for tracking completion
        all_instances_received_buffer: asyncio.Future = asyncio.get_running_loop().create_future()
        
        # Send the buffer descriptor
        await self._send_buffer_descriptor(tensor, expected_msg_key)

        # Compress the tensor for network transmission
        byte_buffer = self._tensor_to_compressed_bytes(tensor)

        # Create an emitter
        emitter = Emitter(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            all_instances_received_buffer,
            byte_buffer,
            to_instance_ids,
        )

        # Register the emitter's callbacks
        self._register_delegates(emitter.handle_message, None, emitter.tick)

        # Start the emitter and wait for completion
        await emitter.begin()
        await all_instances_received_buffer

        # Clear the callbacks
        self._clear_delegates()
        
        # Return the original tensor
        return tensor

    def _tensor_to_compressed_bytes(self, tensor: torch.Tensor) -> bytes:
        """
        Compress a tensor to bytes for efficient network transmission.
        
        Args:
            tensor: The tensor to compress
            
        Returns:
            The compressed tensor as bytes
        """
        from io import BytesIO
        from PIL import Image

        # Log tensor information
        logger.info(f"Compressing tensor with shape {tensor.shape}")

        # Store tensor metadata for reconstruction
        compressed_data = {}

        try:
            # Handle tensor with shape [batch, height, width, channels]
            if len(tensor.shape) == 4:
                batch_size = tensor.shape[0]
                images_data = []

                for i in range(batch_size):
                    img_tensor = tensor[i]
                    # Convert to numpy and ensure values are in valid range for PNG
                    img_np = img_tensor.detach().cpu().numpy()
                    # Scale to 0-255 range for PNG
                    img_np = np.clip(img_np * 255.0, 0, 255).astype(np.uint8)
                    # Create PIL image and compress to PNG in memory
                    img = Image.fromarray(img_np)
                    buffer = BytesIO()
                    img.save(buffer, format="PNG", optimize=True)
                    images_data.append(buffer.getvalue())

                compressed_data["images"] = images_data
                compressed_data["is_batch"] = True
                compressed_data["shape"] = tensor.shape

            # Handle tensor with shape [height, width, channels]
            elif len(tensor.shape) == 3:
                img_np = tensor.detach().cpu().numpy()
                img_np = np.clip(img_np * 255.0, 0, 255).astype(np.uint8)
                img = Image.fromarray(img_np)
                buffer = BytesIO()
                img.save(buffer, format="PNG", optimize=True)

                compressed_data["images"] = [buffer.getvalue()]
                compressed_data["is_batch"] = False
                compressed_data["shape"] = tensor.shape

            else:
                # For unsupported tensor shapes, fall back to binary serialization
                logger.info(
                    f"Tensor shape {tensor.shape} not suitable for PNG compression, using direct binary"
                )
                return tensor.cpu().numpy().tobytes()

            # Use msgpack for more efficient serialization than pickle
            import msgpack

            # Pack the data
            packed_data = msgpack.packb(compressed_data, use_bin_type=True)
            
            # Log payload size
            logger.info(f"Compressed tensor payload size: {len(packed_data)} bytes")
            return packed_data

        except Exception as e:
            logger.error(f"PNG compression failed: {str(e)}\n{traceback.format_exc()}")
            return tensor.cpu().numpy().tobytes()

    def _compressed_bytes_to_tensor(self, buffer: bytes) -> torch.Tensor:
        """
        Convert compressed bytes back to a tensor.
        
        Args:
            buffer: The compressed bytes
            
        Returns:
            The reconstructed tensor
        """
        from io import BytesIO
        from PIL import Image

        try:
            # Try to unpack as msgpack
            import msgpack

            try:
                logger.info(f"Decompressing tensor payload size: {len(buffer)} bytes")
                data = msgpack.unpackb(buffer, raw=False)

                if isinstance(data, dict) and "shape" in data:
                    if data.get("is_batch", False):
                        # Reconstruct batch of images
                        tensors = []
                        for img_data in data["images"]:
                            buffer = BytesIO(img_data)
                            img = Image.open(buffer)
                            img_np = np.array(img).astype("float32") / 255.0
                            tensors.append(torch.from_numpy(img_np))

                        # Stack into batch
                        return torch.stack(tensors, dim=0)
                    else:
                        # Reconstruct single image
                        buffer = BytesIO(data["images"][0])
                        img = Image.open(buffer)
                        img_np = np.array(img).astype("float32") / 255.0
                        return torch.from_numpy(img_np)
            except Exception as inner_e:
                logger.warning(
                    f"Msgpack unpacking failed, trying binary fallback: {str(inner_e)}"
                )
                raise  # Re-raise to fall through to the binary fallback

        except Exception as e:
            logger.warning(
                f"PNG decompression failed, falling back to binary: {str(e)}"
            )

            # Fallback to binary deserialization
            try:
                # Create a writable copy of the array
                array = np.frombuffer(buffer, dtype=np.float32).copy()
                return torch.from_numpy(array)
            except Exception as e2:
                logger.error(f"Failed to deserialize buffer: {str(e2)}")
                raise

    def split_batch(
        self, tensor: torch.Tensor, instance_id: int
    ) -> Tuple[int, int]:
        """
        Split a batch tensor for distribution across instances.
        
        Args:
            tensor: The tensor to split
            instance_id: The instance ID to calculate slice for
            
        Returns:
            Tuple of (start_index, end_index) for the instance's slice
        """
        batch_size = tensor.shape[0]
        instance_count = EnvVars.get_instance_count()
        base_per_instance = batch_size // instance_count
        remainder = batch_size % instance_count

        # Calculate start index
        start_idx = instance_id * base_per_instance + min(instance_id, remainder)
        
        # Calculate end index (add extra item from remainder if applicable)
        extra = 1 if instance_id < remainder else 0
        end_idx = start_idx + base_per_instance + extra

        # Log batch splitting information
        logger.info(
            f"Instance {instance_id}: Splitting batch of size {batch_size} into {instance_count} parts. "
            f"Assigned slice [{start_idx}:{end_idx}] (size: {end_idx - start_idx})"
            f"{' with extra item from remainder' if extra else ''}"
        )

        return start_idx, end_idx

    async def _begin_fanout_emitter(self, tensor: torch.Tensor) -> torch.Tensor:
        """
        Fan out a tensor to all instances in the cluster.
        
        Args:
            tensor: The tensor to fan out
            
        Returns:
            The portion of the tensor for this instance
        """
        # Calculate indices for current instance
        instance_id = EnvVars.get_instance_index()
        start_idx, end_idx = self.split_batch(tensor, instance_id)

        # Extract the tensor slice for the current instance
        output = tensor[start_idx:end_idx]

        # Send slices to other instances
        for instance_id in range(EnvVars.get_instance_count()):
            # Skip this instance
            if instance_id == EnvVars.get_instance_index():
                continue

            # Calculate indices for target instance
            start_idx, end_idx = self.split_batch(tensor, instance_id)

            # Extract the appropriate slice for this instance
            image_tensor = tensor[start_idx:end_idx]
            
            # Send the buffer descriptor
            await self._send_buffer_descriptor(image_tensor, FANOUT_EXPECTED_MSG_KEY)
            
            # Compress the tensor
            byte_buffer = self._tensor_to_compressed_bytes(image_tensor)

            # Create a future for tracking completion
            all_instances_received_buffer = asyncio.get_running_loop().create_future()
            
            # Create an emitter for this instance
            emitter = Emitter(
                self._instance.cluster.udp_message_handler,
                self._instance.cluster.udp_buffer_handler,
                asyncio.get_running_loop(),
                all_instances_received_buffer,
                byte_buffer,
                to_instance_ids=instance_id,
            )

            # Register the emitter's callbacks
            self._register_delegates(
                emitter.handle_message, None, emitter.tick
            )

            # Start the emitter and wait for completion
            await emitter.begin()
            await all_instances_received_buffer

            # Clear the callbacks
            self._clear_delegates()

        # Return the portion for this instance
        return output

    async def _begin_gather_tensors(
        self, tensor: torch.Tensor
    ) -> Tuple[List[bytes], List[Tuple[int, ...]]]:
        """
        Gather tensors from all instances in the cluster.
        
        Args:
            tensor: The tensor to gather
            
        Returns:
            Tuple of (buffer_list, shape_list) for all gathered tensors
        """
        # Compress this instance's tensor
        byte_buffer = self._tensor_to_compressed_bytes(tensor)

        # Create a future for tracking completion
        all_instances_received_buffer = asyncio.get_running_loop().create_future()

        # Create an emitter for broadcasting this instance's tensor
        emitter = Emitter(
            self._instance.cluster.udp_message_handler,
            self._instance.cluster.udp_buffer_handler,
            asyncio.get_running_loop(),
            all_instances_received_buffer,
            byte_buffer,
        )

        # Create futures and receivers for all instances
        on_instance_received_buffer = [None] * EnvVars.get_instance_count()
        receivers = [None] * EnvVars.get_instance_count()

        # Track received buffers and shapes
        received_buffers = [None] * EnvVars.get_instance_count()
        received_buffer_shapes = [None] * EnvVars.get_instance_count()
        
        # Set this instance's buffer and shape
        received_buffers[EnvVars.get_instance_index()] = byte_buffer
        received_buffer_shapes[EnvVars.get_instance_index()] = tensor.shape

        # Create receivers for all instances
        for instance_index in range(EnvVars.get_instance_count()):
            on_instance_received_buffer[instance_index] = asyncio.get_running_loop().create_future()
            receivers[instance_index] = Receiver(
                self._instance.cluster.udp_message_handler,
                self._instance.cluster.udp_buffer_handler,
                asyncio.get_running_loop(),
                on_instance_received_buffer[instance_index],
            )

        # Process each instance (emitting our tensor and receiving others)
        for current_emitter_instance_id in range(EnvVars.get_instance_count()):
            if current_emitter_instance_id == EnvVars.get_instance_index():
                # Register the emitter's callbacks
                self._register_delegates(
                    emitter.handle_message, None, emitter.tick
                )

                # Send the buffer descriptor and begin transmission
                await self._send_buffer_descriptor(tensor, GATHER_EXPECTED_MSG_KEY)
                await emitter.begin()
                await all_instances_received_buffer

                # Clear the callbacks
                self._clear_delegates()
            else:
                # Register the receiver's callbacks
                self._register_delegates(
                    receivers[current_emitter_instance_id].handle_message,
                    receivers[current_emitter_instance_id].handle_buffer,
                    receivers[current_emitter_instance_id].tick,
                )

                # Receive the buffer descriptor and buffer
                buffer_descriptor = await self._receive_buffer_descriptor(GATHER_EXPECTED_MSG_KEY)
                await receivers[current_emitter_instance_id].begin()

                # Get the result
                result = await on_instance_received_buffer[current_emitter_instance_id]
                buffer = result.get_buffer()
                
                # Check for empty buffer
                if len(buffer) == 0:
                    raise RuntimeError(f"Failed to receive buffer from instance {current_emitter_instance_id}")
                
                # Store the buffer and shape
                received_buffers[current_emitter_instance_id] = buffer
                received_buffer_shapes[current_emitter_instance_id] = tuple(buffer_descriptor.buffer_shape)

                # Clear the callbacks
                self._clear_delegates()

        # Return all buffers and shapes
        return received_buffers, received_buffer_shapes

    async def begin_tensor_broadcast(self, tensor: torch.Tensor) -> torch.Tensor:
        """
        Broadcast a tensor to all instances in the cluster.
        
        Args:
            tensor: The tensor to broadcast
            
        Returns:
            The broadcast tensor
        """
        logger.info(f"Broadcasting tensor of shape {tensor.shape}")
        return await self._begin_buffer_sender(tensor, FANOUT_EXPECTED_MSG_KEY)

    async def _send_buffer_descriptor(
        self, tensor: torch.Tensor, expected_key: int
    ) -> None:
        """
        Send a buffer descriptor message.
        
        Args:
            tensor: The tensor to describe
            expected_key: The message key for the operation
        """
        message = ClusterDistributeBufferDescriptor()
        message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_DESCRIPTOR
        message.header.require_ack = True
        message.buffer_shape.extend(list(tensor.shape))
        await self._instance.cluster.udp_message_handler.send_expected_message_thread_safe(
            message, expected_key
        )

    async def _receive_buffer_descriptor(
        self, expected_key: int
    ) -> ClusterDistributeBufferDescriptor:
        """
        Receive a buffer descriptor message.
        
        Args:
            expected_key: The message key for the operation
            
        Returns:
            The buffer descriptor message
        """
        result = await self._instance.cluster.udp_message_handler.await_expected_message_thread_safe(
            expected_key
        )
        if not result.success or not result.data:
            raise RuntimeError("Failed to receive buffer descriptor")

        incoming_message: IncomingMessage = result.data
        buffer_descriptor = ParseDict(
            incoming_message.message, ClusterDistributeBufferDescriptor()
        )

        if not buffer_descriptor.buffer_shape or len(buffer_descriptor.buffer_shape) == 0:
            raise RuntimeError("Invalid buffer descriptor - missing shape information")

        logger.debug(
            f"Received buffer descriptor from instance {incoming_message.sender_instance_id} "
            f"for tensor with shape: {buffer_descriptor.buffer_shape}"
        )
        return buffer_descriptor

    async def begin_fanout_emitter(self, tensor: torch.Tensor) -> torch.Tensor:
        """
        Fan out a tensor to all instances in the cluster.
        
        Args:
            tensor: The tensor to fan out
            
        Returns:
            The portion of the tensor for this instance
        """
        logger.info(f"Distributing tensor of shape {tensor.shape} using fan-out")
        return await self._begin_fanout_emitter(tensor)

    async def begin_receiver(self, expected_msg_key: int) -> torch.Tensor:
        """
        Receive a tensor from another instance.
        
        Args:
            expected_msg_key: The message key for the operation
            
        Returns:
            The received tensor
        """
        # Receive the buffer descriptor
        buffer_descriptor = await self._receive_buffer_descriptor(expected_msg_key)

        # Receive the buffer
        buffer = await self._receive()

        # Convert the buffer to a tensor
        tensor = self._compressed_bytes_to_tensor(buffer)

        # Verify shape matches buffer descriptor and reshape if needed
        expected_shape = tuple(buffer_descriptor.buffer_shape)
        if tensor.shape != expected_shape:
            logger.warning(
                f"Decompressed tensor shape {tensor.shape} doesn't match expected shape {expected_shape}. Reshaping..."
            )
            try:
                tensor = tensor.reshape(expected_shape)
            except RuntimeError as e:
                logger.error(f"Failed to reshape tensor: {str(e)}")
                # In case of critical reshape error, create a new tensor of correct shape
                logger.warning("Creating new tensor with correct shape. Data may be corrupted.")
                tensor = torch.zeros(expected_shape, dtype=tensor.dtype, device=tensor.device)

        logger.info(f"Received tensor of shape {tensor.shape}")
        return tensor

    async def begin_fanin_receiver(self, tensor: torch.Tensor) -> torch.Tensor:
        """
        Receive a tensor from a fan-in operation and combine with local tensor.
        
        Args:
            tensor: The local tensor to combine with received tensor
            
        Returns:
            The combined tensor
        """
        # Receive the tensor
        received_tensor = await self.begin_receiver(FANIN_EXPECTED_MSG_KEY)
        
        # Combine with local tensor
        return torch.cat([tensor, received_tensor], dim=0)

    async def begin_fanout_receiver(self) -> torch.Tensor:
        """
        Receive a tensor from a fan-out operation.
        
        Returns:
            The received tensor
        """
        return await self.begin_receiver(FANOUT_EXPECTED_MSG_KEY)

    async def begin_sender(
        self, tensor: torch.Tensor, to_instance_ids: Optional[List[int]] = None
    ) -> torch.Tensor:
        """
        Send a tensor to specific instances.
        
        Args:
            tensor: The tensor to send
            to_instance_ids: The IDs of instances to send to (None for all)
            
        Returns:
            The sent tensor
        """
        return await self._begin_buffer_sender(
            tensor, FANIN_EXPECTED_MSG_KEY, to_instance_ids
        )

    async def begin_gathering_tensors(self, tensor: torch.Tensor) -> List[torch.Tensor]:
        """
        Gather tensors from all instances in the cluster.
        
        Args:
            tensor: The local tensor to gather with others
            
        Returns:
            List of gathered tensors from all instances
        """
        logger.info(f"Gathering tensors of shape {tensor.shape}")

        # Get all buffers and shapes
        buffers, shapes = await self._begin_gather_tensors(tensor)

        # Convert each buffer into a tensor with its original shape
        tensors = []
        for buffer, shape in zip(buffers, shapes):
            try:
                # Convert buffer to tensor
                decompressed_tensor = self._compressed_bytes_to_tensor(buffer)

                # Ensure the shape matches what we expect
                if decompressed_tensor.shape != shape:
                    logger.warning(
                        f"Decompressed tensor shape {decompressed_tensor.shape} doesn't match expected shape {shape}. Reshaping..."
                    )
                    try:
                        decompressed_tensor = decompressed_tensor.reshape(shape)
                    except RuntimeError as e:
                        logger.error(f"Failed to reshape tensor: {str(e)}")
                        # Create a zero tensor in case of reshape error
                        decompressed_tensor = torch.zeros(
                            shape,
                            dtype=decompressed_tensor.dtype,
                            device=decompressed_tensor.device,
                        )

                # Add batch dimension if needed
                if len(decompressed_tensor.shape) < 4:
                    decompressed_tensor = decompressed_tensor.unsqueeze(0)

                tensors.append(decompressed_tensor)
            except Exception as e:
                logger.error(f"Error decompressing tensor: {str(e)}")
                # Create a zero tensor with correct shape in case of error
                zero_tensor = torch.zeros(shape, dtype=torch.float32)
                if len(zero_tensor.shape) < 4:
                    zero_tensor = zero_tensor.unsqueeze(0)
                tensors.append(zero_tensor)

        return tensors