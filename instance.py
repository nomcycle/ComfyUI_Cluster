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
    ClusterDistributePrompt, ClusterRole,
    ClusterRequestState
)

from .cluster import Cluster
from .states.state_result import StateResult
from .env_vars import EnvVars
from .queued import IncomingMessage, IncomingPacket, IncomingBuffer
from .expected_msg import EXPECT_DISTRIBUTE_PROMPT_MSG_KEY

if TYPE_CHECKING:
    from .instance_loop import InstanceLoop

class QueuedMessage:
    def __init__(self, msg_type_str: str, message, addr: str):
        self.msg_type_str: str = msg_type_str
        self.message = message
        self.addr: str = addr

class Instance:
    def __init__(self, role: int, address: str):
        self.role: int = role
        self.address: str = address
        self.all_accounted_for: bool = False

class OtherInstance(Instance):
    def __init__(self, role: ClusterRole, address: str, direct_port: int, instance_id: int):
        super().__init__(role, address)
        self.direct_port = direct_port
        self.instance_id = instance_id

class ThisInstance(Instance):
    def __init__(self, cluster: Cluster, instance_loop: 'InstanceLoop', role: ClusterRole, address: str, on_hot_reload):
        super().__init__(role, address)
        self.cluster = cluster
        self._msg_queue = queue.Queue()
        self._instance_loop = instance_loop
        self._on_host_reload = on_hot_reload
        # self._pending_state_request_msg: IncomingMessage | None = None
        # self._queued_state_request_msgs: queue.Queue[IncomingMessage] = queue.Queue()

        if EnvVars.get_hot_reload():
            from .states.signal_hot_reload_state import SignalHotReloadStateHandler
            self._signal_hot_reload_state_handler = SignalHotReloadStateHandler(self, self._on_host_reload)
            self._current_state = ClusterState.INITIALIZE
        else:
            from .states.announce_state_handler import AnnounceInstanceStateHandler
            self._current_state_handler = AnnounceInstanceStateHandler(self)
            self._current_state = ClusterState.POPULATING

    def buffer_queue_empty(self) -> bool:
        return self._instance_loop.buffer_queue_empty()

    def _build_url (self, addr: str, endpoint: str):
        return f"http://{addr}:{EnvVars.get_comfy_port()}/{endpoint}"

    async def distribute_prompt(self, prompt_json):

        logger.info("Received request to distribute prompt")

        # while self._current_state != ClusterState.IDLE:
        #     logger.info("Instance is in state: %s, waiting for idle...", self._current_state)
        #     await asyncio.sleep(0.5)

        message = ClusterDistributePrompt()
        message.header.type = ClusterMessageType.DISTRIBUTE_PROMPT
        message.header.require_ack = True
        message.prompt = json.dumps(prompt_json)
        await self.cluster.udp_message_handler.send_and_wait_thread_safe(message)

    async def change_cluster_state(self, state: ClusterState):
        result = await self.cluster.udp_message_handler.request_state_thread_safe(state)
        if not result.success:
            return

    # async def _setup_sync_state(self) -> object:
    #     from .states.sync_state import SyncStateHandler
    #     sync_state_handler = SyncStateHandler(self)
    #     self._current_state_handler = sync_state_handler
    #     self._current_state = ClusterState.EXECUTING
    #     return sync_state_handler
    
    async def broadcast_tensor(self, tensor):
        # sync_state_handler = await self._setup_sync_state()
        return await self._current_state_handler.begin_tensor_broadcast(tensor)

    async def fanout_tensor(self, tensor):
        # sync_state_handler = await self._setup_sync_state()
        return await self._current_state_handler.begin_fanout_emitter(tensor)

    async def receive_tensor_fanout(self):
        # sync_state_handler = await self._setup_sync_state()
        return await self._current_state_handler.begin_fanout_receiver()

    async def gather_tensors(self, tensor):
        # sync_state_handler = await self._setup_sync_state()
        return await self._current_state_handler.begin_gathering_tensors(tensor)

    async def _change_state(self, incoming_msg: IncomingMessage):
        state_request = ParseDict(incoming_msg.message, ClusterRequestState())
        if state_request.state == ClusterState.IDLE:
            from .states.idle_state import IdleStateHandler
            self._current_state = state_request.state
            self._current_state_handler = IdleStateHandler(self)
        elif state_request.state == ClusterState.EXECUTING:
            from .states.sync_handlers import SyncStateHandler
            self._current_state = state_request.state
            self._current_state_handler = SyncStateHandler(self)
        result = await self.cluster.udp_message_handler.resolve_state_thread_safe(state_request, incoming_msg.message_id, incoming_msg.sender_instance_id)
        if not result.success:
            return result

    def handle_state_result(self, state_result: StateResult):
        # logger.info('Tick handle_state_result')
        if state_result is not None and state_result.next_state is not None:
            if state_result.next_state != self._current_state:
                logger.debug("State transition: %s -> %s", self._current_state, state_result.next_state)
                self._current_state = state_result.next_state
                self._current_state_handler = state_result.next_state_handler

    async def handle_state(self):
        if not self._current_state_handler.check_current_state(self._current_state):
            return
        # await self.poll_state_requests()
        state_result: StateResult = await self._current_state_handler.handle_state(self._current_state)
        self.handle_state_result(state_result)

    async def handle_buffer(self, incoming_buffer: IncomingBuffer):
        if not self._current_state_handler.check_current_state(self._current_state):
            return
        # await self.poll_state_requests()
        state_result = await self._current_state_handler.handle_buffer(self._current_state, incoming_buffer)
        self.handle_state_result(state_result)


    async def handle_message(self, incoming_message: IncomingMessage):
        if incoming_message.msg_type == ClusterMessageType.REQUEST_STATE:
            await self._change_state(incoming_message)
            return
            # self._queued_state_request_msgs.put_nowait(incoming_message)

        if not self._current_state_handler.check_message_type(incoming_message.msg_type):
            return

        # await self.poll_state_requests()
        state_result = await self._current_state_handler.handle_message(self._current_state, incoming_message)
        self.handle_state_result(state_result)

class ThisLeaderInstance(ThisInstance):
    def __init__(self, cluster: Cluster, instance_loop: 'InstanceLoop', role: ClusterRole, address: str, on_hot_reload):
        super().__init__(cluster, instance_loop, role, address, on_hot_reload)

class ThisFollowerInstance(ThisInstance):
    def __init__(self, cluster: Cluster, instance_loop: 'InstanceLoop', role: ClusterRole ,address: str, on_hot_reload):
        super().__init__(cluster, instance_loop, role, address, on_hot_reload)

class OtherLeaderInstance(OtherInstance):
    def __init__(self, role: ClusterRole, address: str, direct_port: int, instance_id: int):
        super().__init__(role, address, direct_port, instance_id)

class OtherFollowerInstance(OtherInstance):
    def __init__(self, role: ClusterRole, address: str, direct_port: int, instance_id: int):
        super().__init__(role, address, direct_port, instance_id)