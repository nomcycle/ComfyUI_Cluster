import asyncio

from abc import abstractmethod
from ...queued import IncomingMessage, IncomingBuffer
from ..state_result import StateResult

from ...udp_handle_message import UDPMessageHandler
from ...udp_handle_buffer import UDPBufferHandler


class SyncHandler:
    HEADER_SIZE = 12  # 4 bytes each for buffer flag, instance_id and chunk id
    UDP_MTU = 1460

    def __init__(
        self,
        udp_message_handler: UDPMessageHandler,
        udp_buffer_handler: UDPBufferHandler,
        asyncio_loop: asyncio.AbstractEventLoop,
    ):
        self._udp_message_handler = udp_message_handler
        self._udp_buffer_handler = udp_buffer_handler
        self._async_loop = asyncio_loop

    async def _fence_instances(self) -> bool:
        result = await self._udp_message_handler.await_fence_thread_safe(69)
        if not result.success:
            raise Exception("Failed to fence instances")

    @abstractmethod
    async def begin(self):
        pass

    @abstractmethod
    async def handle_message(
        self, current_state: int, incoming_message: IncomingMessage
    ) -> StateResult | None:
        pass

    @abstractmethod
    async def handle_buffer(
        self, current_state: int, incoming_buffer: IncomingBuffer
    ) -> StateResult | None:
        pass

    @abstractmethod
    async def tick(self):
        pass
