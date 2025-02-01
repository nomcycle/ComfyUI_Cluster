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
from .state_result import StateResult
from ..env_vars import EnvVars

from ..instance import ThisInstance, OtherInstance, OtherLeaderInstance, OtherFollowerInstance
from ..queued import IncomingMessage
from ..udp_base import UDPSingleton

class AnnounceInstanceStateHandler(StateHandler):
    def __init__(self, instance: 'ThisInstance'):
        super().__init__(instance, ClusterState.POPULATING, ClusterMessageType.ANNOUNCE)

    def send_announce(self):
        announce = ClusterAnnounceInstance()
        announce.header.type = ClusterMessageType.ANNOUNCE
        announce.role = self._instance.role
        announce.all_accounted_for = self._instance.cluster.all_accounted_for()
        
        logger.info("Announcing instance '%s' (role=%s)", EnvVars.get_instance_index(), self._instance.role)
        self._instance.cluster.udp_message_handler.send_no_wait(announce)

    async def handle_state(self, current_state: int) -> StateResult:
        if not EnvVars.get_udp_broadcast():
            for instance_id, instance_addr in UDPSingleton.get_cluster_instance_addresses():
                self.register_instance(ClusterRole.LEADER if instance_id == 0 else ClusterRole.FOLLOWER, instance_id, instance_addr, True)
 
            return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))
        self.send_announce()
        await asyncio.sleep(3)

    def register_instance(self, role, instance_id, instance_addr, all_accounted_for: bool):
        other_instance = None

        if role == ClusterRole.LEADER:
            other_instance = OtherLeaderInstance(instance_addr, role, instance_id)
        else: 
            other_instance = OtherFollowerInstance(instance_addr, role, instance_id)

        other_instance.all_accounted_for = all_accounted_for
        self._instance.cluster.instances[instance_id] = other_instance
        return other_instance

    async def handle_message(self, current_state: int, incoming_message: IncomingMessage) -> StateResult | None:
        announce_instance = ParseDict(incoming_message.message, ClusterAnnounceInstance())
        other_instance: OtherInstance = None

        if incoming_message.sender_instance_id in self._instance.cluster.instances:
            other_instance = self._instance.cluster.instances[incoming_message.sender_instance_id]
            other_instance.all_accounted_for = announce_instance.all_accounted_for
        else:
            logger.info("New cluster instance '%s' discovered at %s", incoming_message.sender_instance_id, incoming_message.sender_addr)

            self.register_instance(announce_instance.role, incoming_message.sender_instance_id, incoming_message.sender_addr, announce_instance.all_accounted_for)

            if self._instance.cluster.all_accounted_for():
                logger.info("All cluster instances connected (%d total)", self._instance.cluster.instance_count)
                addresses = []
                addresses.append((EnvVars.get_instance_index(), EnvVars.get_listen_address()))
                addresses.extend(sorted([(instance_id, instance.address) for instance_id, instance in self._instance.cluster.instances.items()]))
                UDPSingleton.set_cluster_instance_addresses(addresses)

        other_instance.all_accounted_for = announce_instance.all_accounted_for
        all_instances_all_accounted_for = all(instance.all_accounted_for for instance in self._instance.cluster.instances.values())
        if self._instance.cluster.all_accounted_for() and all_instances_all_accounted_for:
            logger.info("All instances are populated - transitioning to IDLE state")
            self.send_announce()
            return StateResult(current_state, self, ClusterState.IDLE, IdleStateHandler(self._instance))