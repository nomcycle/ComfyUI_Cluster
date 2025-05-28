import socket
import os
from typing import Optional, List, Tuple
from .protobuf.messages_pb2 import ClusterRole
from .log import logger


class EnvVars:
    _instance_count = None
    _instance_role = None
    _instance_index = None
    _udp_broadcast = None
    _udp_hostnames = None
    _single_host = None
    _direct_listen_port = None
    _broadcast_port = None
    _listen_address = None
    _instance_address = None  # New field for external/public address
    _comfy_port = None
    _registration_mode = None
    _stun_server_url = None
    _cluster_id = None
    _cluster_key = None
    _hot_reload = None

    @classmethod
    def load(cls):
        # Parse instance count
        instance_count = os.getenv("COMFY_CLUSTER_INSTANCE_COUNT")
        if instance_count is None:
            raise Exception(
                "COMFY_CLUSTER_INSTANCE_COUNT environment variable must be set"
            )
        try:
            cls._instance_count = int(instance_count)
        except ValueError:
            raise Exception(
                "COMFY_CLUSTER_INSTANCE_COUNT must be an integer value"
            )

        # Parse instance index
        instance_index = os.getenv("COMFY_CLUSTER_INSTANCE_INDEX")
        if instance_index is None:
            raise Exception(
                "COMFY_CLUSTER_INSTANCE_INDEX environment variable must be set"
            )
        try:
            cls._instance_index = int(instance_index)
        except ValueError:
            raise Exception(
                "COMFY_CLUSTER_INSTANCE_INDEX must be an integer value"
            )
        if (
            cls._instance_index < 0
            or cls._instance_index >= cls._instance_count
        ):
            raise Exception(
                f"COMFY_CLUSTER_INSTANCE_INDEX must be between 0 and {cls._instance_count - 1}"
            )

        # Parse instance role
        instance_role = os.getenv("COMFY_CLUSTER_ROLE")
        if instance_role not in ["LEADER", "FOLLOWER"]:
            raise Exception(
                "COMFY_CLUSTER_ROLE environment variable must be either 'LEADER' or 'FOLLOWER'"
            )
        cls._instance_role = (
            ClusterRole.LEADER
            if instance_role == "LEADER"
            else ClusterRole.FOLLOWER
        )

        # Parse registration mode
        registration_mode = os.getenv("COMFY_CLUSTER_REGISTRATION_MODE", "broadcast")
        if registration_mode not in ["stun", "broadcast", "static"]:
            raise Exception(
                "COMFY_CLUSTER_REGISTRATION_MODE must be one of: stun, broadcast, static"
            )
        cls._registration_mode = registration_mode

        # Parse STUN server URL if using STUN mode
        if registration_mode == "stun":
            stun_server_url = os.getenv("COMFY_CLUSTER_STUN_SERVER")
            if stun_server_url is None:
                raise Exception(
                    "COMFY_CLUSTER_STUN_SERVER environment variable must be set when using 'stun' registration mode"
                )
            cls._stun_server_url = stun_server_url

            # Parse cluster ID if using STUN mode
            cls._cluster_id = os.getenv("COMFY_CLUSTER_ID", "default")
            cls._cluster_key = os.getenv("COMFY_CLUSTER_KEY", "default")

        # Parse UDP broadcast flag (only relevant for broadcast mode)
        cls._udp_broadcast = (
            os.getenv("COMFY_CLUSTER_UDP_BROADCAST", "false").lower() == "true"
            or registration_mode == "broadcast"
        )

        # Parse UDP hostnames (only relevant for static mode)
        cls._udp_hostnames = []
        if registration_mode == "static":
            hostnames = os.getenv("COMFY_CLUSTER_UDP_HOSTNAMES")
            if hostnames is None:
                raise Exception(
                    "COMFY_CLUSTER_UDP_HOSTNAMES environment variable must be set when using 'static' registration mode"
                )
            for i, h in enumerate(hostnames.split(",")):
                hostname, port = h.split(":")
                cls._udp_hostnames.append(
                    (i, hostname.strip(), int(port.strip()))
                )

        # Parse single host flag
        cls._single_host = (
            os.getenv("COMFY_CLUSTER_SINGLE_HOST", "false").lower() == "true"
        )
        
        # Parse hot reload flag
        cls._hot_reload = (
            os.getenv("COMFY_CLUSTER_HOT_RELOAD", "false").lower() == "true"
        )

        # Parse listen address (internal address to bind to)
        cls._listen_address = os.getenv(
            "COMFY_CLUSTER_LISTEN_ADDRESS", "0.0.0.0"
        )
        
        # Parse instance address (external/public address for other instances to connect to)
        instance_address = os.getenv("COMFY_CLUSTER_INSTANCE_ADDRESS")
        
        # Handle RunPod special case
        if os.getenv("RUNPOD_POD_ID"):
            # Log that we're using RunPod environment
            # Check if RUNPOD_POD_ID environment variable exists
            instance_address = socket.gethostbyname(f"{os.getenv('RUNPOD_POD_ID')}.runpod.internal")
            logger.info(f"RUNPOD environment detected, using runpod internal address: {instance_address}")

        # If instance address is not set, default to listen address (except for 0.0.0.0)
        elif instance_address is None:
            # If listen address is 0.0.0.0, try to get the hostname
            if cls._listen_address == "0.0.0.0":
                try:
                    instance_address = socket.gethostbyname(socket.gethostname())
                    logger.info(f"No instance address specified, using hostname: {instance_address}")
                except Exception:
                    # Fall back to localhost if hostname lookup fails
                    instance_address = "localhost"
                    logger.warning(f"Failed to get hostname, using default: {instance_address}")
            else:
                # Use the listen address as instance address
                instance_address = cls._listen_address
        
        cls._instance_address = instance_address
        logger.info(f"Instance address: {cls._instance_address} (listen address: {cls._listen_address})")

        # Parse listen port
        direct_listen_port = os.getenv(
            "COMFY_CLUSTER_DIRECT_LISTEN_PORT", "9997"
        )
        try:
            cls._direct_listen_port = int(direct_listen_port)
        except ValueError:
            raise Exception(
                "COMFY_CLUSTER_DIRECT_LISTEN_PORT must be an integer value"
            )

        # Parse send port
        broadcast_port = os.getenv("COMFY_CLUSTER_BROADCAST_PORT", "9997")
        try:
            cls._broadcast_port = int(broadcast_port)
        except ValueError:
            raise Exception("COMFY_CLUSTER_BROADCAST_PORT must be an integer value")

        # Parse ComfyUI port
        comfy_port = os.getenv("COMFY_CLUSTER_COMFY_PORT", "8188")
        try:
            cls._comfy_port = int(comfy_port)
        except ValueError:
            raise Exception(
                "COMFY_CLUSTER_COMFY_PORT must be an integer value"
            )

    @classmethod
    def get_instance_count(cls) -> int:
        """Get the total number of instances in the cluster."""
        if cls._instance_count is None:
            cls.load()
        return cls._instance_count

    @classmethod
    def get_instance_index(cls) -> int:
        """Get the index of this instance."""
        if cls._instance_index is None:
            cls.load()
        return cls._instance_index

    @classmethod
    def get_instance_role(cls) -> ClusterRole:
        """Get the role of this instance."""
        if cls._instance_role is None:
            cls.load()
        return cls._instance_role

    @classmethod
    def get_registration_mode(cls) -> str:
        """Get the registration mode (stun, broadcast, static)."""
        if cls._registration_mode is None:
            cls.load()
        return cls._registration_mode

    @classmethod
    def get_stun_server_url(cls) -> Optional[str]:
        """Get the STUN server URL."""
        if cls._stun_server_url is None and cls._registration_mode is None:
            cls.load()
        return cls._stun_server_url

    @classmethod
    def get_cluster_id(cls) -> str:
        """Get the cluster ID."""
        if cls._cluster_id is None and cls._registration_mode is None:
            cls.load()
        return cls._cluster_id or "default"

    @classmethod
    def get_cluster_key(cls) -> str:
        """Get the cluster key."""
        if cls._cluster_key is None and cls._registration_mode is None:
            cls.load()
        return cls._cluster_key or "default"

    @classmethod
    def get_udp_broadcast(cls) -> bool:
        """Get whether UDP broadcast is enabled."""
        if cls._udp_broadcast is None:
            cls.load()
        return cls._udp_broadcast

    @classmethod
    def get_udp_hostnames(cls) -> List[Tuple[int, str, int]]:
        """Get the list of UDP hostnames."""
        if cls._udp_hostnames is None:
            cls.load()
        return cls._udp_hostnames

    @classmethod
    def get_single_host(cls) -> bool:
        """Get whether this is a single host deployment."""
        if cls._single_host is None:
            cls.load()
        return cls._single_host

    @classmethod
    def get_hot_reload(cls) -> bool:
        """Get whether hot reload is enabled."""
        if cls._hot_reload is None:
            cls.load()
        return cls._hot_reload

    @classmethod
    def get_listen_address(cls) -> str:
        """Get the address to listen on (internal address)."""
        if cls._listen_address is None:
            cls.load()
        return cls._listen_address

    @classmethod
    def get_instance_address(cls) -> str:
        """Get the address for other instances to connect to (external/public address)."""
        if cls._instance_address is None:
            cls.load()
        return cls._instance_address

    @classmethod
    def get_direct_listen_port(cls) -> int:
        """Get the port to listen on for direct communication."""
        if cls._direct_listen_port is None:
            cls.load()
        return cls._direct_listen_port

    @classmethod
    def get_broadcast_port(cls) -> int:
        """Get the port to use for broadcast communication."""
        if cls._broadcast_port is None:
            cls.load()
        return cls._broadcast_port

    @classmethod
    def get_comfy_port(cls) -> int:
        """Get the port that ComfyUI is running on."""
        if cls._comfy_port is None:
            cls.load()
        return cls._comfy_port