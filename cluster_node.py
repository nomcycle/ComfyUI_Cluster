import asyncio
import uuid
import socket
import ipaddress
import threading
import time

from .log import logger
from .listener import UDPListener
from .sender import UDPSender
from google.protobuf.json_format import ParseDict
from .protobuf.messages_pb2 import ClusterMessageType, ClusterFenceRequest, ClusterFenceResponse, ClusterAnnounceInstance, ClusterSignalHotReload

global hot_reload_iteration
hot_reload_iteration = 0

class ClusterNode:
    _instance = None
    _announce_thread = None
    _listener_thread = None
    _hot_reload_timestamp = time.time()

    def __init__(self) -> None:
        if ClusterNode._instance is not None:
            raise Exception("ClusterNode is a singleton - use ClusterNode.get_instance()")
        
        logger.info("Initializing cluster node")
        self._listener = UDPListener(message_callback=self.handle_message)
        self._sender = UDPSender()
        self._instance_id = str(uuid.uuid4())
        logger.info(f"Generated instance ID: {self._instance_id}")
        self._running = True

        signal_hot_reload = ClusterSignalHotReload()
        signal_hot_reload.type = ClusterMessageType.SIGNAL_HOT_RELOAD
        signal_hot_reload.timestamp = str(time.time())
        self._sender.send(signal_hot_reload)
        
        ClusterNode._announce_thread = threading.Thread(target=self._announce_loop, daemon=True)
        ClusterNode._listener_thread = threading.Thread(target=self._listener_loop, daemon=True)
        
        ClusterNode._announce_thread.start()
        ClusterNode._listener_thread.start()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _listener_loop(self):
        while self._running:
            self._listener.poll()

    def _announce_loop(self):
        time.sleep(3)
        while self._running:
            announce = ClusterAnnounceInstance()
            announce.type = ClusterMessageType.ANNOUNCE
            announce.instance_id = self._instance_id
            self._sender.send(announce)
            time.sleep(1)

    def handle_message(self, message, addr):
        msg_type = ClusterMessageType.Value(message.get('type'))
        if msg_type == ClusterMessageType.ANNOUNCE:
            cluster_announce_instance = ParseDict(message, ClusterAnnounceInstance())
            if cluster_announce_instance.instance_id == self._instance_id:
                return
        elif msg_type == ClusterMessageType.SIGNAL_HOT_RELOAD:
            signal_hot_reload = ParseDict(message, ClusterSignalHotReload())
            logger.info(f"Received hot reload signal with timestamp {signal_hot_reload.timestamp}")
            timestamp = float(signal_hot_reload.timestamp)
            if timestamp > self._hot_reload_timestamp + 5:
                logger.info("Received hot reload signal, shutting down to make way for new iteration of this class.")
                self._running = False
            return

        logger.error(f"Unhandled message type: {msg_type}")

class InstanceNode(ClusterNode):
    def __init__(self) -> None:
        super().__init__()
    
    def handle_message(self, message, addr):
        pass

class Leader(ClusterNode):
    def __init__(self) -> None:
        super().__init__()
    
    def handle_message(self, message, addr):
        pass

class Follower(ClusterNode):
    def __init__(self) -> None:
        super().__init__()
    
    def handle_message(self, message, addr):
        pass