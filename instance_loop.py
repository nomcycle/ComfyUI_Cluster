import asyncio
import uuid
import threading

from .log import logger
from .udp import UDP
from .env_vars import EnvVars

from .protobuf.messages_pb2 import ClusterRole

from .cluster import Cluster
from .instance import ThisInstance, ThisLeaderInstance, ThisFollowerInstance

class InstanceLoop:
    _instance = None

    def __init__(self) -> None:
        if InstanceLoop._instance is not None:
            raise Exception("ClusterNode is a singleton - use ClusterNode.get_instance()")

        self._instance_role = EnvVars.get_instance_role()
        self._instance_id = str(uuid.uuid4())
        logger.info("Instance initialized with ID: %s", self._instance_id)

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
        del self._this_instance.cluster.udp
        del self._this_instance.cluster
        del self._this_instance

    def _run_state_loop(self):
        try:
            self._state_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._state_loop)

            udp: UDP = UDP(self._handle_message, self._state_loop, self._instance_id)
            self._cluster = Cluster(udp)
            if self._instance_role == ClusterRole.LEADER:
                self._this_instance = ThisLeaderInstance(self._cluster, self._instance_id, 'localhost', ClusterRole.LEADER, self._on_hot_reload)
            else:
                self._this_instance = ThisFollowerInstance(self._cluster, self._instance_id, 'localhost', ClusterRole.FOLLOWER, self._on_hot_reload)
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

    def _handle_message(self, msg_type_str: str, message, addr: str):
        if not self._running:
            return

        try:
            self._this_instance.handle_message(msg_type_str, message, addr)
        except Exception as e:
            logger.error("Message handling failed: %s", str(e), exc_info=True)

class LeaderInstanceLoop(InstanceLoop):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _handle_message(self, msg_type, message, addr):
        super()._handle_message(msg_type, message, addr)

class FollowerInstanceLoop(InstanceLoop):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _handle_message(self, msg_type, message, addr):
        super()._handle_message(msg_type, message, addr)

def create_instance_loop():
    return LeaderInstanceLoop.get_instance() if EnvVars.get_instance_role() == ClusterRole.LEADER else FollowerInstanceLoop.get_instance()