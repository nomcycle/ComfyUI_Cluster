"""
Cluster module - Handles instance coordination and configuration for the ComfyUI cluster.
"""
from typing import Dict, List, Tuple, Optional

from .log import logger
from .udp.udp_base import UDPSingleton
from .udp.udp_handle_message import UDPMessageHandler
from .udp.udp_handle_buffer import UDPBufferHandler
from .env_vars import EnvVars

class Cluster:
    """
    Manages cluster configuration and tracks connected instances.
    
    This class is responsible for maintaining the list of connected instances,
    tracking when all instances are accounted for, and providing access to
    the UDP communication handlers.
    """
    
    def __init__(self):
        """Initialize the cluster with environment configuration."""
        # Set up instance tracking
        self.instances: Dict[int, "OtherInstance"] = {}
        self.instance_count: int = EnvVars.get_instance_count()
        
        # Handler references (will be set later)
        self._udp_message_handler: Optional[UDPMessageHandler] = None
        self._udp_buffer_handler: Optional[UDPBufferHandler] = None
        
        # Initialize with pre-configured addresses if UDP broadcast is disabled
        if not EnvVars.get_udp_broadcast():
            self._expected_instances = EnvVars.get_udp_hostnames()
            logger.info(
                "Awaiting instances (UDP broadcast disabled): %s",
                self._expected_instances,
            )
            UDPSingleton.set_cluster_instance_addresses(self._expected_instances)
            
        logger.info("Expected instance count: %d", self.instance_count)

    @property
    def udp_message_handler(self) -> UDPMessageHandler:
        """Get the UDP message handler for the cluster."""
        if self._udp_message_handler is None:
            raise RuntimeError("UDP message handler not initialized")
        return self._udp_message_handler
        
    @property
    def udp_buffer_handler(self) -> UDPBufferHandler:
        """Get the UDP buffer handler for the cluster."""
        if self._udp_buffer_handler is None:
            raise RuntimeError("UDP buffer handler not initialized")
        return self._udp_buffer_handler

    def set_udp_message_handler(self, handler: UDPMessageHandler) -> None:
        """Set the UDP message handler for the cluster."""
        self._udp_message_handler = handler

    def set_udp_buffer_handler(self, handler: UDPBufferHandler) -> None:
        """Set the UDP buffer handler for the cluster."""
        self._udp_buffer_handler = handler

    def all_accounted_for(self) -> bool:
        """Check if all expected instances have been registered."""
        return len(self.instances) == self.instance_count - 1
        
    def update_instance_addresses(self, instance_index: int, listen_address: str, 
                                  direct_listen_port: int) -> None:
        """
        Update the UDP system with all known instance addresses.
        
        This is called when all instances have been discovered to ensure
        proper addressing for direct communication.
        """
        # Combine the current instance with all registered instances
        addresses = [
            (instance_index, listen_address, direct_listen_port)
        ]
        
        # Add all other instances
        addresses.extend(
            (instance_id, instance.address, instance.direct_port)
            for instance_id, instance in self.instances.items()
        )
        
        # Sort by instance ID for consistency
        addresses.sort(key=lambda x: x[0])
        
        # Update the UDP singleton with all addresses
        UDPSingleton.set_cluster_instance_addresses(addresses)