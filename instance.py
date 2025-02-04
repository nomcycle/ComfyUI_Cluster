from typing import Dict, TYPE_CHECKING
import json
from google.protobuf.json_format import ParseDict

import queue
import asyncio
import random
import sys
import math
from .log import logger
from .protobuf.messages_pb2 import (
    ClusterState, ClusterMessageType, 
    ClusterDistributePrompt, ClusterRole
)

from .cluster import Cluster
from .states.state_result import StateResult
from .env_vars import EnvVars
from .queued import IncomingMessage, IncomingPacket
if TYPE_CHECKING:
    from .instance_loop import InstanceLoop

class QueuedMessage:
    def __init__(self, msg_type_str: str, message, addr: str):
        self.msg_type_str: str = msg_type_str
        self.message = message
        self.addr: str = addr

class Instance:
    def __init__(self, address: str, role: int):
        self.all_accounted_for: bool = False
        self.address: str = address
        self.role: int = role

class OtherInstance(Instance):
    def __init__(self, address: str, role: int, instance_id: int):
        super().__init__(address, role)
        self.instance_id = instance_id

class ThisInstance(Instance):
    def __init__(self, cluster: Cluster, instance_loop: 'InstanceLoop', address: str, role: int, on_hot_reload):
        super().__init__(address, role)
        self.cluster = cluster
        self._msg_queue = queue.Queue()
        self._instance_loop = instance_loop

        if EnvVars.get_hot_reload():
            from .states.signal_hot_reload_state import SignalHotReloadStateHandler
            self._signal_hot_reload_state_handler = SignalHotReloadStateHandler(self, on_hot_reload)
            self._current_state = ClusterState.INITIALIZE
        else:
            from .states.announce_state_handler import AnnounceInstanceStateHandler
            self._current_state_handler = AnnounceInstanceStateHandler(self)
            self._current_state = ClusterState.POPULATING

    async def tick_state(self):

        if not self._current_state_handler.check_current_state(self._current_state):
            return

        state_result: StateResult = await self._current_state_handler.handle_state(self._current_state)
        self.handle_state_result(state_result)

    def buffer_queue_empty(self) -> bool:
        return self._instance_loop.buffer_queue_empty()

    def _build_url (self, addr: str, endpoint: str):
        return f"http://{addr}:{EnvVars.get_comfy_port()}/{endpoint}"

    async def distribute_prompt(self, prompt_json):

        logger.info("Received request to distribute prompt")

        while self._current_state != ClusterState.IDLE:
            logger.info("Instance is in state: %s, waiting for idle...", self._current_state)
            await asyncio.sleep(0.5)

        message = ClusterDistributePrompt()
        message.header.type = ClusterMessageType.DISTRIBUTE_PROMPT
        message.header.require_ack = True
        message.prompt = json.dumps(prompt_json)
        await self.cluster.udp_message_handler.send_and_wait_thread_safe(message)

    async def fanin_tensor(self, tensor):
        from .states.executing_state import ExecutingStateHandler
        if self.role == ClusterRole.LEADER:
            self._current_state = ClusterState.EXECUTING
            self._current_state_handler = ExecutingStateHandler(self)

        while self._current_state != ClusterState.EXECUTING:
            logger.info("Instance is in state: %s, waiting for execution...", self._current_state)
            await asyncio.sleep(0.5)
        executing_state_handler: ExecutingStateHandler = self._current_state_handler
        return await executing_state_handler.distribute_tensor(tensor)

    def handle_state_result(self, state_result):
        # logger.info('Tick handle_state_result')
        if state_result is None or state_result.next_state is None:
            return

        if state_result.next_state != self._current_state:
            logger.debug("State transition: %s -> %s", self._current_state, state_result.next_state)
            self._current_state = state_result.next_state
            self._current_state_handler = state_result.next_state_handler

    async def handle_buffer(self, incoming_packet: IncomingPacket):
        # logger.info('Tick handle_buffer')

        if self._current_state != ClusterState.EXECUTING:
            logger.debug("Instance not in EXECUTING state, dropping buffer")
            return
        state_result = await self._current_state_handler.handle_buffer(self._current_state, incoming_packet.packet, incoming_packet.sender_addr)
        self.handle_state_result(state_result)

    async def handle_message(self, incoming_message: IncomingMessage):
        # logger.info('Tick handle_message')

        if incoming_message.msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            await self._signal_hot_reload_state_handler.handle_message(self._current_state, incoming_message)
            return

        if not self._current_state_handler.check_message_type(incoming_message.msg_type):
            return

        state_result = await self._current_state_handler.handle_message(self._current_state, incoming_message)
        self.handle_state_result(state_result)

class ThisLeaderInstance(ThisInstance):
    def __init__(self, cluster: Cluster, instance_loop: 'InstanceLoop',address: str, role: int, on_hot_reload):
        super().__init__(cluster, instance_loop, address, role, on_hot_reload)

class ThisFollowerInstance(ThisInstance):
    def __init__(self, cluster: Cluster, instance_loop: 'InstanceLoop',address: str, role: int, on_hot_reload):
        super().__init__(cluster, instance_loop, address, role, on_hot_reload)

class OtherLeaderInstance(OtherInstance):
    def __init__(self, address: str, role: int, instance_id: int):
        super().__init__(address, role, instance_id)

class OtherFollowerInstance(OtherInstance):
    def __init__(self, address: str, role: int, instance_id: int):
        super().__init__(address, role, instance_id)