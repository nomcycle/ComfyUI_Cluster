import os
from .log import logger
from .protobuf.messages_pb2 import ClusterRole

class EnvVars:
    _instance_count = None
    _instance_role = None
    _instance_index = None
    _udp_broadcast = None
    _udp_hostnames = None
    _single_host = None
    _listen_port = None
    _send_port = None
    _listen_address = None
    _comfy_port = None

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

        # Parse instance index
        instance_index = os.getenv('COMFY_CLUSTER_INSTANCE_INDEX')
        if instance_index is None:
            raise Exception("COMFY_CLUSTER_INSTANCE_INDEX environment variable must be set")
        try:
            cls._instance_index = int(instance_index)
        except ValueError:
            raise Exception("COMFY_CLUSTER_INSTANCE_INDEX must be an integer value")
        if cls._instance_index < 0 or cls._instance_index >= cls._instance_count:
            raise Exception(f"COMFY_CLUSTER_INSTANCE_INDEX must be between 0 and {cls._instance_count - 1}")

        # Parse instance role
        instance_role = os.getenv('COMFY_CLUSTER_ROLE')
        if instance_role not in ["LEADER", "FOLLOWER"]:
            raise Exception("COMFY_CLUSTER_ROLE environment variable must be either 'LEADER' or 'FOLLOWER'")
        cls._instance_role = ClusterRole.LEADER if instance_role == 'LEADER' else ClusterRole.FOLLOWER

        # Parse UDP broadcast flag
        cls._udp_broadcast = os.getenv('COMFY_CLUSTER_UDP_BROADCAST') is not None

        # Parse UDP hostnames
        cls._udp_hostnames = []
        if not cls._udp_broadcast:
            hostnames = os.getenv('COMFY_CLUSTER_UDP_HOSTNAMES')
            if hostnames is not None:
                cls._udp_hostnames = [h.strip() for h in hostnames.split(',')]

        # Parse single host flag
        cls._single_host = os.getenv('COMFY_CLUSTER_SINGLE_HOST', 'false').lower() == 'true'

        # Parse listen address
        cls._listen_address = os.getenv('COMFY_CLUSTER_LISTEN_ADDRESS', '0.0.0.0')

        # Parse listen port
        listen_port = os.getenv('COMFY_CLUSTER_LISTEN_PORT', '9997')
        try:
            cls._listen_port = int(listen_port)
        except ValueError:
            raise Exception("COMFY_CLUSTER_LISTEN_PORT must be an integer value")

        # Parse send port
        send_port = os.getenv('COMFY_CLUSTER_SEND_PORT', '9997')
        try:
            cls._send_port = int(send_port)
        except ValueError:
            raise Exception("COMFY_CLUSTER_SEND_PORT must be an integer value")

        # Parse ComfyUI port
        comfy_port = os.getenv('COMFY_CLUSTER_COMFY_PORT', '8188')
        try:
            cls._comfy_port = int(comfy_port)
        except ValueError:
            raise Exception("COMFY_CLUSTER_COMFY_PORT must be an integer value")

    @classmethod
    def get_instance_count(cls):
        if cls._instance_count is None:
            cls.load()
        return cls._instance_count

    @classmethod
    def get_instance_index(cls):
        if cls._instance_index is None:
            cls.load()
        return cls._instance_index

    @classmethod
    def get_instance_role(cls):
        if cls._instance_role is None:
            cls.load()
        return cls._instance_role

    @classmethod
    def get_udp_broadcast(cls) -> bool:
        if cls._udp_broadcast is None:
            cls.load()
        return cls._udp_broadcast

    @classmethod
    def get_udp_hostnames(cls) -> [str]:
        if cls._udp_hostnames is None:
            cls.load()
        return cls._udp_hostnames

    @classmethod
    def get_single_host(cls) -> bool:
        if cls._single_host is None:
            cls.load()
        return cls._single_host

    @classmethod
    def get_listen_address(cls) -> str:
        if cls._listen_address is None:
            cls.load()
        return cls._listen_address

    @classmethod
    def get_listen_port(cls) -> int:
        if cls._listen_port is None:
            cls.load()
        return cls._listen_port

    @classmethod
    def get_send_port(cls) -> int:
        if cls._send_port is None:
            cls.load()
        return cls._send_port

    @classmethod
    def get_comfy_port(cls) -> int:
        if cls._comfy_port is None:
            cls.load()
        return cls._comfy_port
