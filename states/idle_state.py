import asyncio
from typing import Dict, TYPE_CHECKING

from ..log import logger
from google.protobuf.json_format import ParseDict
from ..protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, ClusterSignalIdle
)

from .state_handler import StateHandler
from .state_result import StateResult

from ..instance import Instance

class IdleStateHandler(StateHandler):
    def __init__(self, instance: Instance):
        super().__init__(instance, ClusterState.IDLE, ClusterMessageType.SIGNAL_IDLE)

    async def handle_state(self, current_state: int) -> StateResult | None:
        signal_idle = ClusterSignalIdle()
        signal_idle.header.type = ClusterMessageType.SIGNAL_IDLE
        signal_idle.header.require_ack = True
        
        logger.info("Sending idle signal to followers")
        await self._instance.cluster.udp.send_and_wait(signal_idle)
        await asyncio.sleep(3)

    def handle_message(self, current_state: int, message, addr: str) -> StateResult | None:
        signal_idle = ParseDict(message, ClusterSignalIdle())
        sender_id = signal_idle.header.sender_instance_id
        
        if sender_id not in self._instance.cluster.instances:
            logger.error(f"Invalid idle signal from unknown instance {sender_id}")
            return
            
        logger.info(f"Received idle signal from leader {sender_id}")