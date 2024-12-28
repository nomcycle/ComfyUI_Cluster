import asyncio
import uuid
import socket
import ipaddress
import threading
import time
import os

from .log import logger
from .udp import UDP
from .env_vars import EnvVars
from google.protobuf.json_format import ParseDict
from .protobuf.messages_pb2 import *

global hot_reload_iteration
hot_reload_iteration = 0

class Instance:
    def __init__(self, instance_id, address, role):
        self._instance_id = instance_id
        self._address = address
        self._role = role

class LeaderInstance(Instance):
    def __init__(self, instance_id, address, role):
        super().__init__(instance_id, address, role)

class FollowerInstance(Instance):
    def __init__(self, instance_id, address, role):
        super().__init__(instance_id, address, role)

class ClusterNode:
    _instance = None

    def __init__(self) -> None:
        if ClusterNode._instance is not None:
            raise Exception("ClusterNode is a singleton - use ClusterNode.get_instance()")

        self._instance_count = EnvVars.get_instance_count()
        logger.info(f"Cluster instance count: {self._instance_count}")
        self._instance_role = EnvVars.get_instance_role()

        logger.info("Initializing cluster node")
        self._instance_id = str(uuid.uuid4())
        logger.info(f"Generated instance ID: {self._instance_id}")
        self._udp = UDP(self._handle_message)
        self._instances = {}

        self._current_state = ClusterState.POPULATING
        self._state_thread = threading.Thread(target=self._state_loop, daemon=True)
        self._state_thread.start()
    
    def _change_state(self, next_state):
        self._current_state = next_state

    def _handle_announce_state(self):
        time.sleep(1)
        announce = ClusterAnnounceInstance()
        announce.type = ClusterMessageType.ANNOUNCE
        announce.instance_id = self._cluster_instance_id
        announce.role = self._instance_role
        self._udp.send(announce)

    def _handle_ready_state(self):
        raise NotImplementedError("_handle_ready_state not implemented")

    def _state_loop(self):
        if self._current_state == ClusterState.POPULATING:
            self._handle_announce_state()
            return
        elif self._current_state == ClusterState.READY:
            return

    def _handle_announce_instance_message(self, message, addr):
        announce_instance = ParseDict(message, ClusterAnnounceInstance())
        if announce_instance.instance_id == self._instance_id:
            return

        if announce_instance.instance_id not in self._instances:
            logger.info(f"Discovered new cluster instance {announce_instance.instance_id} at {addr}")

            role = ClusterRole.Value(announce_instance.role)
            another_instance = None

            if role == ClusterRole.LEADER:
                another_instance = LeaderInstance(announce_instance.instance_id, addr, role)
            else: another_instance = FollowerInstance(announce_instance.instance_id, addr, role)

            self._instances[announce_instance.instance_id] = another_instance

            if len(self._instances) == self._instance_count - 1:  # -1 because we don't count ourselves
                logger.info(f"All {self._instance_count} cluster instances are now connected")
                self._change_state(ClusterState.READY)

    def _handle_signal_hot_reload_message (self, message):
        signal_hot_reload = ParseDict(message, ClusterSignalHotReload())
        logger.info(f"Received hot reload signal with timestamp {signal_hot_reload.timestamp}")
        timestamp = float(signal_hot_reload.timestamp)
        if timestamp > self._hot_reload_timestamp + 5:
            logger.info("Received hot reload signal, shutting down to make way for new iteration of this class.")
            self._running = False

    def _handle_message(self, msg_type, message, addr):
        if msg_type == ClusterMessageType.ANNOUNCE:
            self._handle_announce_instance_message(message, addr)
            return
        elif msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            self._handle_signal_hot_reload_message(message)
            return

        logger.error(f"Unhandled message type: {msg_type}")

class Leader(ClusterNode):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _handle_ready_state(self):
        pass
    
    def _handle_message(self, msg_type, message, addr):
        pass

class Follower(ClusterNode):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _handle_ready_state(self):
        pass
    
    def _handle_message(self, msg_type, message, addr):
        pass

def create_cluster_instance_node():
    return Leader.get_instance() if EnvVars.get_instance_role() == ClusterRole.LEADER else Follower.get_instance()