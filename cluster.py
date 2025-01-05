from typing import Dict

from .log import logger
from .udp import UDP
from .env_vars import EnvVars

class Cluster:
    def __init__(self, udp: UDP):
        self.udp = udp
        if not EnvVars._udp_broadcast:
            self._expected_instances = EnvVars.get_udp_hostnames()
            logger.info('Awaiting instances (UDP broadcast disabled): %s', self._expected_instances)
            udp.set_cluster_instance_addresses(self._expected_instances)

        self.expected_instances: [str] = []
        self.instances: Dict[str, 'OtherInstance'] = {}  # Fixed type hint
        self.instance_count: int = EnvVars.get_instance_count()
        logger.info('Expected instance count: %d', self.instance_count)

    def all_accounted_for(self) -> bool:
        return len(self.instances) == self.instance_count - 1