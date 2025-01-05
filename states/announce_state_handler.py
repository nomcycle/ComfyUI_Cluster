import asyncio
from typing import Dict, TYPE_CHECKING

from google.protobuf.json_format import ParseDict
from ..log import logger
from ..protobuf.messages_pb2 import (
    ClusterRole, ClusterState, ClusterMessageType,
    ClusterAnnounceInstance, ClusterSignalIdle
)

from .state_handler import StateHandler
from .idle_state import IdleStateHandler
from ..instance import Instance, LeaderInstance, FollowerInstance
from .state_result import StateResult

from ..instance import Instance, LeaderInstance, FollowerInstance

class AnnounceInstanceStateHandler(StateHandler):
    def __init__(self, instance: 'Instance'):
        super().__init__(instance, ClusterState.POPULATING, ClusterMessageType.ANNOUNCE)

    def send_announce(self):
        announce = ClusterAnnounceInstance()
        announce.header.type = ClusterMessageType.ANNOUNCE
        announce.role = self._instance.role
        announce.all_accounted_for = self._instance.cluster.all_accounted_for()
        
        logger.info("Announcing instance '%s' (role=%s)", self._instance.instance_id, self._instance.role)
        self._instance.cluster.udp.send_no_wait(announce)

    async def handle_state(self, current_state: int) -> StateResult:
        self.send_announce()
        await asyncio.sleep(3)

    def handle_message(self, current_state: int, message, addr: str) -> StateResult | None:
        announce_instance = ParseDict(message, ClusterAnnounceInstance())
        another_instance: Instance = None

        if announce_instance.header.sender_instance_id in self._instance.cluster.instances:
            another_instance = self._instance.cluster.instances[announce_instance.header.sender_instance_id]
            another_instance.all_accounted_for = announce_instance.all_accounted_for
        else:
            logger.info("New cluster instance '%s' discovered at %s", announce_instance.header.sender_instance_id, addr)

            role = announce_instance.role
            another_instance = None

            if role == ClusterRole.LEADER:
                another_instance = LeaderInstance(self._instance.cluster, announce_instance.header.sender_instance_id, addr, role)
            else: 
                another_instance = FollowerInstance(self._instance.cluster, announce_instance.header.sender_instance_id, addr, role)

            another_instance.all_accounted_for = announce_instance.all_accounted_for
            self._instance.cluster.instances[announce_instance.header.sender_instance_id] = another_instance
            self._instance.cluster.expected_instances.append(addr)

            if self._instance.cluster.all_accounted_for():
                logger.info("All cluster instances connected (%d total)", self._instance.cluster.instance_count)
                self._instance.cluster.udp.set_cluster_instance_addresses(self._instance.cluster.expected_instances)

        another_instance.all_accounted_for = announce_instance.all_accounted_for
        all_instances_all_accounted_for = all(instance.all_accounted_for for instance in self._instance.cluster.instances.values())
        if self._instance.cluster.all_accounted_for() and all_instances_all_accounted_for:
            logger.info("All instances are populated - transitioning to IDLE state")
            self.send_announce()
            return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))