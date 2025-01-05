from typing import Dict, TYPE_CHECKING
from google.protobuf.json_format import ParseDict

import queue
from .log import logger
from .protobuf.messages_pb2 import ClusterState, ClusterMessageType, ClusterRole

from .cluster import Cluster
from .states.state_result import StateResult

class QueuedMessage:
    def __init__(self, msg_type_str: str, message, addr: str):
        self.msg_type_str: str = msg_type_str
        self.message = message
        self.addr: str = addr

class Instance:
    def __init__(self, cluster: Cluster, instance_id: str, address: str, role: int):
        self.cluster = cluster
        self.all_accounted_for: bool = False
        self.instance_id: str = instance_id
        self.address: str = address
        self.role: int = role
        self._msg_queue = queue.Queue()
        if self.role == ClusterRole.LEADER:
            self._current_state = ClusterState.INITIALIZE
            from .states.signal_hot_reload_state import SignalHotReloadStateHandler
            self._signal_hot_reload_state_handler = SignalHotReloadStateHandler(self) 
            self._current_state_handler = self._signal_hot_reload_state_handler
        else:
            self._current_state = ClusterState.POPULATING
            from .states.announce_state_handler import AnnounceInstanceStateHandler
            self._current_state_handler = AnnounceInstanceStateHandler(self)

    async def tick_state(self):
        if not self._current_state_handler.check_current_state(self._current_state):
            return
        state_result: StateResult = await self._current_state_handler.handle_state(self._current_state)
        self.handle_state_result(state_result)

    def handle_state_result(self, state_result):
        if state_result is None or state_result.next_state is None:
            return

        if state_result.next_state != self._current_state:
            logger.debug("State transition: %s -> %s", self._current_state, state_result.next_state)
            self._current_state = state_result.next_state
            self._current_state_handler = state_result.next_state_handler

    def handle_message(self, msg_type_str: str, message, addr: str):
        msg_type = ClusterMessageType.Value(msg_type_str)
        if self.role == ClusterRole.FOLLOWER and msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            state_result = self._signal_hot_reload_state_handler.handle_message(self._current_state, message, addr)
            self.handle_state_result(state_result)
            return

        if not self._current_state_handler.check_message_type(msg_type):
            return

        state_result = self._current_state_handler.handle_message(self._current_state, message, addr)
        self.handle_state_result(state_result)

class LeaderInstance(Instance):
    def __init__(self, cluster: Cluster, instance_id: str, address: str, role: int):
        super().__init__(cluster, instance_id, address, role)

class FollowerInstance(Instance):
    def __init__(self, cluster: Cluster, instance_id: str, address: str, role: int):
        super().__init__(cluster, instance_id, address, role)