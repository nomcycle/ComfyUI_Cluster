"""
Registration strategy module for ComfyUI Cluster.

This module provides different strategies for instance registration and discovery.
"""
from abc import ABC, abstractmethod
import asyncio
import json
from typing import List, Tuple, Dict, Optional, Any
import aiohttp

from .env_vars import EnvVars
from .log import logger
from .protobuf.messages_pb2 import ClusterRole, ClusterMessageType, ClusterAnnounceInstance


class IRegistrationStrategy(ABC):
    """Interface for registration strategies."""
    
    @abstractmethod
    async def discover_instances(self) -> List[Tuple[int, str, int, str]]:
        """
        Discover instances and return a list of (instance_id, address, port, role)
        
        Returns:
            List of tuples containing (instance_id, address, port, role)
        """
        pass


class STUNRegistrationStrategy(IRegistrationStrategy):
    """
    Registration strategy using a STUN server.
    
    This strategy uses a central STUN server for instance registration and discovery.
    """
    
    def __init__(self):
        """Initialize the STUN registration strategy."""
        self._stun_url = EnvVars.get_stun_server_url()
        self._cluster_id = EnvVars.get_cluster_id()
        self._cluster_key = EnvVars.get_cluster_key()
        self._instance_id = EnvVars.get_instance_index()
        self._role = "LEADER" if EnvVars.get_instance_role() == ClusterRole.LEADER else "FOLLOWER"
        self._instance_address = EnvVars.get_instance_address()  # Use instance address instead of listen address
        self._port = EnvVars.get_direct_listen_port()
        
        # Validate configuration
        if not self._stun_url:
            raise ValueError("STUN server URL not configured. Set COMFY_CLUSTER_STUN_SERVER environment variable.")
        if not self._cluster_key:
            raise ValueError("Cluster key not configured. Set COMFY_CLUSTER_KEY environment variable.")
    
    async def discover_instances(self) -> List[Tuple[int, str, int, str]]:
        """
        Discover instances using the STUN server.
        
        Returns:
            List of tuples containing (instance_id, address, port, role)
        """
        # Register this instance first
        await self._register_instance()
        
        # Then get all instances
        return await self._get_all_instances()
    
    async def _register_instance(self) -> None:
        """Register this instance with the STUN server."""
        async with aiohttp.ClientSession() as session:
            try:
                registration_data = {
                    "address": self._instance_address,  # Use instance address instead of listen address
                    "direct_listen_port": self._port,
                    "role": self._role,
                    "instance_id": self._instance_id,
                    "cluster_id": self._cluster_id,
                    "cluster_key": self._cluster_key
                }
                
                logger.info(f"Registering with STUN server: {self._stun_url}")
                logger.debug(f"Registration data: {registration_data}")
                
                async with session.post(
                    f"{self._stun_url}/register-instance",
                    json=registration_data,
                    timeout=10  # 10 seconds timeout
                ) as response:
                    if response.status == 401 or response.status == 403:
                        error_text = await response.text()
                        logger.error(f"Authentication failed with STUN server: {error_text}")
                        raise RuntimeError(f"STUN server authentication failed: {error_text}. Check your COMFY_CLUSTER_KEY setting.")
                    elif response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Failed to register with STUN server: {error_text}")
                        raise RuntimeError(f"STUN server registration failed: {error_text}")
                    
                    result = await response.json()
                    logger.info(f"Registered with STUN server. Total instances: {result.get('instance_count', 0)}")
            
            except aiohttp.ClientError as e:
                logger.error(f"Connection error with STUN server: {str(e)}")
                raise RuntimeError(f"Failed to connect to STUN server: {str(e)}")
            
            except Exception as e:
                logger.error(f"Error registering with STUN server: {str(e)}")
                raise RuntimeError(f"STUN registration error: {str(e)}")
    
    async def _get_all_instances(self) -> List[Tuple[int, str, int, str]]:
        """
        Get all instances from the STUN server.
        
        Returns:
            List of tuples containing (instance_id, address, port, role)
        """
        instances = []
        async with aiohttp.ClientSession() as session:
            try:
                logger.info(f"Getting instances from STUN server for cluster: {self._cluster_id}")
                
                # Include cluster key in header for authentication
                headers = {"X-Cluster-Key": self._cluster_key}
                
                async with session.get(
                    f"{self._stun_url}/instances/{self._cluster_id}",
                    headers=headers,
                    timeout=10  # 10 seconds timeout
                ) as response:
                    if response.status == 401 or response.status == 403:
                        error_text = await response.text()
                        logger.error(f"Authentication failed when getting instances: {error_text}")
                        raise RuntimeError(f"STUN server authentication failed: {error_text}. Check your COMFY_CLUSTER_KEY setting.")
                    elif response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Failed to get instances from STUN server: {error_text}")
                        raise RuntimeError(f"STUN server get instances failed: {error_text}")
                    
                    data = await response.json()
                    
                    for instance in data.get("instances", []):
                        instance_id = instance.get("instance_id")
                        address = instance.get("address")
                        port = instance.get("direct_listen_port")
                        role = instance.get("role")
                        
                        # Validate instance data
                        if None in (instance_id, address, port, role):
                            logger.warning(f"Skipping invalid instance data: {instance}")
                            continue
                        
                        instances.append((instance_id, address, port, role))
                
                logger.info(f"Discovered {len(instances)} instances via STUN server")
                logger.debug(f"Discovered instances: {instances}")
                
            except aiohttp.ClientError as e:
                logger.error(f"Connection error with STUN server when getting instances: {str(e)}")
                raise RuntimeError(f"Failed to get instances from STUN server: {str(e)}")
            
            except Exception as e:
                logger.error(f"Error getting instances from STUN server: {str(e)}")
                raise RuntimeError(f"STUN get instances error: {str(e)}")
        
        return instances


class BroadcastRegistrationStrategy(IRegistrationStrategy):
    """
    Registration strategy using UDP broadcast.
    
    This strategy uses UDP broadcast messages for instance discovery.
    """
    
    def __init__(self):
        """Initialize the broadcast registration strategy."""
        # This is a placeholder. The actual UDP broadcast logic is in instance.py and will be refactored.
        # For now, this is just a wrapper around the existing logic.
        self._received_instances: Dict[int, Tuple[str, int, str]] = {}
        self._discovery_complete = asyncio.Event()
    
    async def discover_instances(self) -> List[Tuple[int, str, int, str]]:
        """
        Discover instances using UDP broadcast.
        
        This is a placeholder that will be refactored to integrate with the existing UDP broadcast logic.
        In the current implementation, instance discovery is handled directly in instance.py.
        
        Returns:
            List of tuples containing (instance_id, address, port, role)
        """
        # This would normally be implemented to return discovered instances,
        # but for now, we'll just return an empty list since the actual discovery
        # is handled elsewhere.
        logger.info("Using existing UDP broadcast mechanism for instance discovery")
        return []


class StaticRegistrationStrategy(IRegistrationStrategy):
    """
    Registration strategy using static configuration.
    
    This strategy uses pre-configured hostnames from environment variables.
    """
    
    def __init__(self):
        """Initialize the static registration strategy."""
        self._hostnames = EnvVars.get_udp_hostnames()
        
        # Validate configuration
        if not self._hostnames:
            raise ValueError("Static hostnames not configured. Set COMFY_CLUSTER_UDP_HOSTNAMES environment variable.")
    
    async def discover_instances(self) -> List[Tuple[int, str, int, str]]:
        """
        Discover instances using static configuration.
        
        Returns:
            List of tuples containing (instance_id, address, port, role)
        """
        instances = []
        
        for instance_id, hostname, port in self._hostnames:
            # Skip this instance
            if instance_id == EnvVars.get_instance_index():
                continue
            
            # Determine role based on instance ID (0 = LEADER, others = FOLLOWER)
            role = "LEADER" if instance_id == 0 else "FOLLOWER"
            
            instances.append((instance_id, hostname, port, role))
        
        logger.info(f"Discovered {len(instances)} instances via static configuration")
        logger.debug(f"Static instances: {instances}")
        
        return instances


def create_registration_strategy() -> IRegistrationStrategy:
    """
    Create a registration strategy based on environment variables.
    
    Returns:
        An implementation of IRegistrationStrategy
    
    Raises:
        ValueError: If the registration mode is invalid or the strategy cannot be created
    """
    mode = EnvVars.get_registration_mode()
    
    try:
        if mode == "stun":
            return STUNRegistrationStrategy()
        elif mode == "static":
            return StaticRegistrationStrategy()
        elif mode == "broadcast":
            return BroadcastRegistrationStrategy()
        else:
            logger.warning(f"Unknown registration mode: {mode}, falling back to broadcast")
            return BroadcastRegistrationStrategy()
    except Exception as e:
        logger.error(f"Failed to create registration strategy for mode {mode}: {str(e)}")
        raise ValueError(f"Failed to create registration strategy: {str(e)}")