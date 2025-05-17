"""
Cluster module - Handles instance coordination and configuration for the ComfyUI cluster.
"""
import asyncio
from typing import Dict, List, Tuple, Optional, Any

from .log import logger
from .udp.udp_base import UDPSingleton
from .udp.udp_handle_message import UDPMessageHandler
from .udp.udp_handle_buffer import UDPBufferHandler
from .env_vars import EnvVars
from .registration_strategy import create_registration_strategy, IRegistrationStrategy


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
        
        # Create the registration strategy based on environment variables
        self._registration_strategy: Optional[IRegistrationStrategy] = None
        self._registration_complete = False
        
        # Initialize with pre-configured addresses if in static mode
        if EnvVars.get_registration_mode() == "static":
            self._expected_instances = EnvVars.get_udp_hostnames()
            logger.info(
                "Using static registration mode with instances: %s",
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

    def is_registration_complete(self) -> bool:
        """Check if instance registration is complete."""
        return self._registration_complete
        
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
        
    def mark_registration_complete(self) -> None:
        """Mark instance registration as complete."""
        self._registration_complete = True
        logger.info("Instance registration complete. Cluster is ready.")
        
    def get_registration_strategy(self) -> IRegistrationStrategy:
        """
        Get the instance registration strategy.
        
        Returns:
            IRegistrationStrategy: The registration strategy to use.
        """
        if self._registration_strategy is None:
            self._registration_strategy = create_registration_strategy()
        return self._registration_strategy
    
    async def discover_instances(self) -> bool:
        """
        Discover instances using the configured registration strategy.
        
        This is used by the instance initialization process to find other instances
        in the cluster based on the configured registration mode.
        
        Returns:
            bool: True if discovery was successful, False otherwise.
        """
        try:
            # Get the registration strategy
            strategy = self.get_registration_strategy()
            
            # Discover instances
            logger.info(f"Discovering instances using {EnvVars.get_registration_mode()} strategy")
            instances = await strategy.discover_instances()
            
            # Register discovered instances
            for instance_id, addr, port, role_str in instances:
                # Skip this instance
                if instance_id == EnvVars.get_instance_index():
                    continue
                
                # Parse role
                from .protobuf.messages_pb2 import ClusterRole
                role = ClusterRole.LEADER if role_str == "LEADER" else ClusterRole.FOLLOWER
                
                # Register the instance
                logger.info(f"Discovered instance {instance_id} at {addr}:{port} with role {role_str}")
                
                # Import here to avoid circular imports
                from .instance import OtherLeaderInstance, OtherFollowerInstance
                
                # Create the appropriate instance type
                if role == ClusterRole.LEADER:
                    other_instance = OtherLeaderInstance(
                        role, addr, port, instance_id
                    )
                else:
                    other_instance = OtherFollowerInstance(
                        role, addr, port, instance_id
                    )
                
                # Add to instance registry
                self.instances[instance_id] = other_instance
                
            # Check if all instances were discovered
            if self.all_accounted_for():
                logger.info("All instances discovered successfully")
                return True
            else:
                logger.warning(
                    f"Not all instances discovered. Found {len(self.instances)}/{self.instance_count-1}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error discovering instances: {str(e)}")
            return False