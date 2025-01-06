
import asyncio
from typing import Dict, TYPE_CHECKING

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType
)

from .state_handler import StateHandler
from .state_result import StateResult
from ..instance import ThisInstance

class DistributePromptHandler(StateHandler):
    def __init__(self, instance: ThisInstance):
        super().__init__(instance, ClusterState.IDLE, ClusterMessageType.SIGNAL_IDLE)

    async def handle_state(self, current_state: int) -> StateResult | None:
        await asyncio.sleep(0.1)

    def handle_message(self, current_state: int, msg_type: int, message, addr: str) -> StateResult | None:
        pass