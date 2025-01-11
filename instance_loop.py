import asyncio
import uuid
import threading

from .log import logger
from .env_vars import EnvVars

from .protobuf.messages_pb2 import ClusterRole

from .cluster import Cluster
from .instance import ThisInstance, ThisLeaderInstance, ThisFollowerInstance
from .udp_base import UDPSingleton
from .udp_handle_message import UDPMessageHandler
from .udp_handle_buffer import UDPBufferHandler

global instance_loop
instance_loop = None

class InstanceLoop:

    def __init__(self) -> None:
        global instance_loop
        if instance_loop is not None:
            raise Exception("ClusterNode is a singleton - use ClusterNode.get_instance()")

        self._instance_role = EnvVars.get_instance_role()

        self._cluster: Cluster = None
        self._this_instance: ThisInstance = None

        self._running = True
        self._state_loop = None
        self._state_lock = threading.Lock()
        self._state_thread = threading.Thread(target=self._run_state_loop, daemon=True)
        self._state_thread.start()

    def _on_hot_reload(self):
        logger.info("Cleaning up...")

        self._running = False
        self._this_instance.cluster.udp.cancel_all_pending()

        UDPSingleton.stop_threads()
        del self._this_instance.cluster.udp
        del self._this_instance.cluster
        del self._this_instance

        global instance_loop
        del instance_loop

    def _run_state_loop(self):
        try:
            self._state_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._state_loop)

            self._cluster = Cluster()
            if self._instance_role == ClusterRole.LEADER:
                self._this_instance = ThisLeaderInstance(self._cluster, 'localhost', ClusterRole.LEADER, self._on_hot_reload)
            else:
                self._this_instance = ThisFollowerInstance(self._cluster, 'localhost', ClusterRole.FOLLOWER, self._on_hot_reload)
            udp_message_handler =  UDPMessageHandler(self._this_instance.handle_message, self._state_loop)
            udp_buffer_handler =  UDPBufferHandler( self._this_instance.handle_buffer, self._state_loop)
            self._cluster.set_udp_message_handler(udp_message_handler)
            self._cluster.set_udp_buffer_handler(udp_buffer_handler)

            UDPSingleton.start_threads()

            self._state_loop.run_until_complete(self._state_loop_async())
        finally:
            self._state_loop.close()
            
    async def _state_loop_async(self):
        try:
            while self._running:
                await self._this_instance.tick_state()
        except Exception as e:
            logger.error("State loop failed: %s", str(e), exc_info=True)
            raise

        logger.info("Exited state loop.")

class LeaderInstanceLoop(InstanceLoop):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def get_instance(cls):
        global instance_loop
        if instance_loop is None:
            instance_loop = cls()
        return instance_loop

    async def _handle_message(self, msg_type, message, addr):
        await super()._handle_message(msg_type, message, addr)

class FollowerInstanceLoop(InstanceLoop):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def get_instance(cls):
        global instance_loop
        if instance_loop is None:
            instance_loop = cls()
        return instance_loop

    async def _handle_message(self, msg_type, message, addr):
        await super()._handle_message(msg_type, message, addr)

def get_instance_loop():
    return LeaderInstanceLoop.get_instance() if EnvVars.get_instance_role() == ClusterRole.LEADER else FollowerInstanceLoop.get_instance()