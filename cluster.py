from typing import Dict

from .log import logger
from .udp.udp_base import UDPSingleton
from .udp.udp_handle_message import UDPMessageHandler
from .udp.udp_handle_buffer import UDPBufferHandler
from .env_vars import EnvVars


class Cluster:
    def __init__(self):
        if not EnvVars._udp_broadcast:
            self._expected_instances = EnvVars.get_udp_hostnames()
            logger.info(
                "Awaiting instances (UDP broadcast disabled): %s",
                self._expected_instances,
            )
            UDPSingleton.set_cluster_instance_addresses(
                self._expected_instances
            )

        self.udp_message_handler: UDPMessageHandler = None
        self.udp_buffer_handler: UDPBufferHandler = None

        self.instances: Dict[int, "OtherInstance"] = {}  # Fixed type hint
        self.instance_count: int = EnvVars.get_instance_count()
        logger.info("Expected instance count: %d", self.instance_count)

    def set_udp_message_handler(self, udp_message_handler: UDPMessageHandler):
        self.udp_message_handler = udp_message_handler

    def set_udp_buffer_handler(self, udp_buffer_handler: UDPBufferHandler):
        self.udp_buffer_handler = udp_buffer_handler

    def all_accounted_for(self) -> bool:
        return len(self.instances) == self.instance_count - 1
