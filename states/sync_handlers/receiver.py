import asyncio
import threading
from typing import Dict, List
from ...udp.queued import IncomingBuffer, IncomingMessage
import time

import numpy as np
from ...log import logger
from ...env_vars import EnvVars

from .sync_handler import SyncHandler
from ...udp.udp_handle_message import UDPMessageHandler
from ...udp.udp_handle_buffer import UDPBufferHandler

from ...udp.expected_msg import BEGIN_BUFFER_EXPECTED_MSG_KEY

from google.protobuf.json_format import ParseDict
from ...protobuf.messages_pb2 import (
    ClusterMessageType,
    ClusterDistributeBufferBegin,
    ClusterDistributeBufferAck,
    ClusterDistributeBufferResend,
)

BUFFER_RESEND_MAX_REQUEST_SIZE = 1024  # bytes


class BufferBeginEvent:
    def __init__(self, sender_instance_id: int, buffer_type: int, chunk_count: int):
        self._sender_instance_id = sender_instance_id
        self._buffer_type = buffer_type
        self._chunk_count = chunk_count


class CompletedBufferEvent:
    def __init__(self, buffer: bytes):
        self._buffer: bytes = buffer

    def get_buffer(self) -> bytes:
        return self._buffer


class Receiver(SyncHandler):
    def __init__(
        self,
        udp_message_handler: UDPMessageHandler,
        udp_buffer_handler: UDPBufferHandler,
        asyncio_loop: asyncio.AbstractEventLoop,
        completed_buffer: asyncio.Future,
    ):
        super().__init__(udp_message_handler, udp_buffer_handler, asyncio_loop)
        self._thread_lock = threading.Lock()
        self._completed_buffer_event: asyncio.Future = completed_buffer
        self._time_since_polling_chunk_progress = time.time()

        self._dependency_chunks: Dict[bytes] = {}
        self._chunks_bitfield = np.array([], dtype=bool)
        self._expected_chunk_ids: List[int] = []
        self._expected_buffer_type: int = -1
        self._sender_instance_id: int = -1
        self._received_begin_buffer_msg: bool = False

    async def begin(self):
        await self._fence_instances()
        result = await self._udp_message_handler.await_expected_message_thread_safe(
            BEGIN_BUFFER_EXPECTED_MSG_KEY
        )
        if not result.success or not result.data:
            raise RuntimeError('Failed to receive buffer begin message')
        buffer_begin_message = result.data

        with self._thread_lock:
            distribute_buffer = ParseDict(
                buffer_begin_message.message, ClusterDistributeBufferBegin()
            )
            logger.debug(
                'Received buffer begin message from instance %s, expecting %s chunks',
                distribute_buffer.instance_index,
                distribute_buffer.chunk_count,
            )
            self._sender_instance_id = distribute_buffer.instance_index
            self._expected_buffer_type = distribute_buffer.buffer_type
            self._expected_chunk_ids = list(range(distribute_buffer.chunk_count))
            self._dependency_chunks = {}
            self._chunks_bitfield = np.zeros(distribute_buffer.chunk_count, dtype=np.bool_)
            self._received_begin_buffer_msg = True

    async def handle_message(
        self, current_state: int, incoming_message: IncomingMessage
    ) -> None:
        pass

    def _buffer_progress(self):
        if len(self._chunks_bitfield) == 0:
            raise ValueError('No chunks field available - cannot calculate buffer progress')
        total_chunks = np.sum(self._chunks_bitfield)  # Count True flags
        expected_total = len(self._expected_chunk_ids) if self._expected_chunk_ids else 0
        return total_chunks, expected_total

    async def tick(self):
        if not self._received_begin_buffer_msg:
            return await asyncio.sleep(0.001)

        with self._thread_lock:
            current_time = time.time()
            incoming_queue_size = self._udp_buffer_handler.get_incoming_buffer_queue_size()
            time_since_last_packet = self._udp_buffer_handler.get_time_since_last_packet()
            time_since_last_poll = current_time - self._time_since_polling_chunk_progress

            if (incoming_queue_size == 0 or
                time_since_last_packet < 0.025 or
                time_since_last_poll < 0.025):
                return await asyncio.sleep(0.001)

            self._time_since_polling_chunk_progress = current_time

            total_chunks, expected_total = self._buffer_progress()
            logger.debug(
                'Instance %s: Have %s/%s chunks',
                EnvVars.get_instance_index(),
                total_chunks,
                expected_total,
            )
            if expected_total > 0:
                if total_chunks < expected_total:
                    first_missing_index = np.argmin(self._chunks_bitfield)

                    # Calculate the end index (don\'t go past the end of the array)
                    end_index = min(
                        first_missing_index + BUFFER_RESEND_MAX_REQUEST_SIZE * 8,
                        len(self._chunks_bitfield),
                    )

                    # Extract the window of missing chunks
                    window_bitfield = ~self._chunks_bitfield[first_missing_index:end_index]
                    # Log the window size we are requesting
                    window_size = end_index - first_missing_index
                    window_bytes = np.packbits(window_bitfield).tobytes()
                    logger.debug(
                        'Requesting resend for window size: %s starting at index %s, byte length: %s',
                        window_size,
                        first_missing_index,
                        len(window_bytes),
                    )

                    # Create and send the resend request
                    message = ClusterDistributeBufferResend()
                    message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_RESEND
                    message.header.require_ack = True
                    message.instance_index = EnvVars.get_instance_index()
                    message.missing_chunk_ids = window_bytes
                    message.window_start = first_missing_index
                    message.window_size = end_index - first_missing_index

                    await self._udp_message_handler.send_and_wait(message, self._sender_instance_id)

                else:
                    message = ClusterDistributeBufferAck()
                    message.header.type = ClusterMessageType.DISTRIBUTE_BUFFER_ACK
                    message.header.require_ack = True
                    message.instance_index = EnvVars.get_instance_index()
                    await self._udp_message_handler.send_and_wait(message, self._sender_instance_id)

                    logger.info(
                        'All chunks received from instance %s, joining buffers',
                        self._sender_instance_id,
                    )
                    # Join chunks in correct order using chunk IDs
                    ordered_chunks = [
                        self._dependency_chunks[chunk_id]
                        for chunk_id in sorted(self._dependency_chunks.keys())
                    ]
                    joined_buffer = b''.join(ordered_chunks)

                    self._async_loop.call_soon_threadsafe(
                        self._completed_buffer_event.set_result,
                        CompletedBufferEvent(joined_buffer),
                    )

    async def handle_buffer(
        self, current_state: int, incoming_buffer: IncomingBuffer
    ) -> None:
        with self._thread_lock:
            sender_instance_id = incoming_buffer.get_sender_instance_id()
            if sender_instance_id == EnvVars.get_instance_index() or sender_instance_id != self._sender_instance_id:
                raise ValueError(
                    'Received buffer from unexpected instance %s, expected %s'
                    % (sender_instance_id, self._sender_instance_id)
                )

            chunk_id = incoming_buffer.get_chunk_id()

            # Check whether we already have that chunk.
            if self._chunks_bitfield[chunk_id]:
                return

            if sender_instance_id is not None:
                buffer_view = memoryview(incoming_buffer.packet)
                self._dependency_chunks[chunk_id] = buffer_view[SyncHandler.HEADER_SIZE:].tobytes()
                self._chunks_bitfield[chunk_id] = True

                # Check progress less frequently
                if chunk_id % 5000 == 0 or chunk_id == len(self._expected_chunk_ids) - 1:
                    total_chunks, expected_total = self._buffer_progress()
                    logger.info(
                        'Received chunk %s from instance %s. Total: %s/%s',
                        chunk_id,
                        sender_instance_id,
                        total_chunks,
                        expected_total,
                    )
