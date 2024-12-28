import os
from .log import logger
from .protobuf.messages_pb2 import ClusterRole

class EnvVars:
    _instance_count = None
    _instance_role = None

    @classmethod
    def load(cls):
        # Parse instance count
        instance_count = os.getenv('COMFY_CLUSTER_INSTANCE_COUNT')
        if instance_count is None:
            raise Exception("COMFY_CLUSTER_INSTANCE_COUNT environment variable must be set")
        try:
            cls._instance_count = int(instance_count)
        except ValueError:
            raise Exception("COMFY_CLUSTER_INSTANCE_COUNT must be an integer value")

        # Parse instance role
        instance_role = os.getenv('COMFY_CLUSTER_ROLE')
        if instance_role not in ["LEADER", "FOLLOWER"]:
            raise Exception("COMFY_CLUSTER_ROLE environment variable must be either 'LEADER' or 'FOLLOWER'")
        cls._instance_role = ClusterRole.LEADER if instance_role == 'LEADER' else ClusterRole.FOLLOWER

    @classmethod
    def get_instance_count(cls):
        if cls._instance_count is None:
            cls.load()
        return cls._instance_count

    @classmethod
    def get_instance_role(cls):
        if cls._instance_role is None:
            cls.load()
        return cls._instance_role
