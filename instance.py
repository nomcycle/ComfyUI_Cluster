from typing import Dict, TYPE_CHECKING
from google.protobuf.json_format import ParseDict

import queue
import json
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

class QueuedMessage:
    def __init__(self, msg_type_str: str, message, addr: str):
        self.msg_type_str: str = msg_type_str
        self.message = message
        self.addr: str = addr

class OtherInstance:
    def __init__(self, instance_id: str, address: str, role: int):
        self.all_accounted_for: bool = False
        self.instance_id: str = instance_id
        self.address: str = address
        self.role: int = role

class ThisInstance:
    def __init__(self, cluster: Cluster, instance_id: str, address: str, role: int, on_hot_reload):
        self.cluster = cluster
        self.all_accounted_for: bool = False
        self.instance_id: str = instance_id
        self.address: str = address
        self.role: int = role
        self._msg_queue = queue.Queue()
        from .states.signal_hot_reload_state import SignalHotReloadStateHandler
        self._signal_hot_reload_state_handler = SignalHotReloadStateHandler(self, on_hot_reload)
        self._current_state = ClusterState.INITIALIZE
        self._current_state_handler = self._signal_hot_reload_state_handler

    async def tick_state(self):
        if not self._current_state_handler.check_current_state(self._current_state):
            return

        state_result: StateResult = await self._current_state_handler.handle_state(self._current_state)
        self.handle_state_result(state_result)

    def _build_url (self, addr: str, endpoint: str):
        return f"http://{addr}:{EnvVars.get_comfy_port()}/{endpoint}"

    async def distribute_prompt(self, prompt_json):
        while self._current_state != ClusterState.IDLE:
            logger.info("Instance is in state: %s, waiting for idle...", self._current_state)
            await asyncio.sleep(0.01)

        message = ClusterDistributePrompt()
        message.header.type = ClusterMessageType.DISTRIBUTE_PROMPT
        message.header.require_ack = True
        message.prompt = json.dumps(prompt_json)
        logger.info("Distributing prompt: %s", message.prompt)
        await self.cluster.udp.send_and_wait_thread_safe(message)

    async def fanin_tensor(self, tensor):
        from .states.executing_state import ExecutingStateHandler
        if self.role == ClusterRole.LEADER:
            self._current_state = ClusterState.EXECUTING
            self._current_state_handler = ExecutingStateHandler(self)

        while self._current_state != ClusterState.EXECUTING:
            logger.info("Instance is in state: %s, waiting for execution...", self._current_state)
            await asyncio.sleep(0.01)
        executing_state_handler: ExecutingStateHandler = self._current_state_handler
        await executing_state_handler.distribute_tensor(tensor)

    def handle_state_result(self, state_result):
        if state_result is None or state_result.next_state is None:
            return

        if state_result.next_state != self._current_state:
            logger.debug("State transition: %s -> %s", self._current_state, state_result.next_state)
            self._current_state = state_result.next_state
            self._current_state_handler = state_result.next_state_handler

    def handle_buffer(self, buffer, addr: str):
        state_result = self._current_state_handler.handle_buffer(self._current_state, buffer, addr)
        self.handle_state_result(state_result)

    def handle_message(self, msg_type_str: str, message, addr: str):

        msg_type = ClusterMessageType.Value(msg_type_str)

        if msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            self._signal_hot_reload_state_handler.handle_message(self._current_state, msg_type, message, addr)
            return

        if not self._current_state_handler.check_message_type(msg_type):
            return

        state_result = self._current_state_handler.handle_message(self._current_state, msg_type, message, addr)
        self.handle_state_result(state_result)

class ThisLeaderInstance(ThisInstance):
    def __init__(self, cluster: Cluster, instance_id: str, address: str, role: int, on_hot_reload):
        super().__init__(cluster, instance_id, address, role, on_hot_reload)

class ThisFollowerInstance(ThisInstance):
    def __init__(self, cluster: Cluster, instance_id: str, address: str, role: int, on_hot_reload):
        super().__init__(cluster, instance_id, address, role, on_hot_reload)

class OtherLeaderInstance(OtherInstance):
    def __init__(self, instance_id: str, address: str, role: int):
        super().__init__(instance_id, address, role)

class OtherFollowerInstance(OtherInstance):
    def __init__(self, instance_id: str, address: str, role: int):
        super().__init__(instance_id, address, role)