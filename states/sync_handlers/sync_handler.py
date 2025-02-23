from abc import abstractmethod
from ...queued import IncomingMessage, IncomingBuffer
from ..state_result import StateResult

from ...udp_handle_message import UDPMessageHandler
from ...udp_handle_buffer import UDPBufferHandler

class SyncHandler:

    HEADER_SIZE = 12  # 4 bytes each for buffer flag, instance_id and chunk id
    UDP_MTU = 1460

    def __init__(self, udp_message_handler: UDPMessageHandler, udp_buffer_handler: UDPBufferHandler):
        self._udp_message_handler = udp_message_handler
        self._udp_buffer_handler = udp_buffer_handler
    
    @abstractmethod
    async def begin(self):
        pass

    @abstractmethod
    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        pass

    @abstractmethod
    async def handle_buffer(self, current_state: int, incoming_buffer: IncomingBuffer) -> StateResult | None:
        pass

    @abstractmethod
    async def tick(self):
        pass