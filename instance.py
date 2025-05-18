"""
Instance module - Defines the different instance types in the ComfyUI cluster.
"""
from typing import TYPE_CHECKING, Dict, List, Optional, Any, Tuple, Callable
import asyncio
import json
import queue
import threading
from datetime import datetime

from google.protobuf.json_format import ParseDict

from .log import logger
from .protobuf.messages_pb2 import (
    ClusterRole,
    ClusterMessageType,
    ClusterAnnounceInstance,
    ClusterDistributePrompt,
    ClusterDistributeBufferDescriptor,
)
from .env_vars import EnvVars
from .udp.queued import IncomingMessage, IncomingBuffer
from .udp.expected_msg import BEGIN_BUFFER_EXPECTED_MSG_KEY, FANIN_EXPECTED_MSG_KEY, FANOUT_EXPECTED_MSG_KEY, GATHER_EXPECTED_MSG_KEY

if TYPE_CHECKING:
    from .cluster import Cluster
    from .instance_loop import InstanceLoop
    from .states.sync_handlers.emitter import Emitter
    from .states.sync_handlers.receiver import Receiver


class Instance:
    """Base class for all instances in the cluster."""
    
    def __init__(self, role: int, address: str):
        """
        Initialize an instance.
        
        Args:
            role: The role of this instance (LEADER or FOLLOWER)
            address: The network address of this instance
        """
        self.role: int = role
        self.address: str = address
        self.all_accounted_for: bool = False


class OtherInstance(Instance):
    """Represents another instance in the cluster."""
    
    def __init__(
        self,
        role: ClusterRole,
        address: str,
        direct_port: int,
        instance_id: int,
    ):
        """
        Initialize another instance.
        
        Args:
            role: The role of this instance (LEADER or FOLLOWER)
            address: The network address of this instance
            direct_port: The direct communication port of this instance
            instance_id: The unique ID of this instance
        """
        super().__init__(role, address)
        self.direct_port: int = direct_port
        self.instance_id: int = instance_id


class ThisInstance(Instance):
    """Represents this instance in the cluster with synchronization capabilities."""
    
    def __init__(
        self,
        cluster: "Cluster",
        instance_loop: "InstanceLoop",
        role: ClusterRole,
        address: str,
        on_hot_reload: Callable[[], None],
    ):
        """
        Initialize this instance.
        
        Args:
            cluster: The cluster this instance belongs to
            instance_loop: The instance loop managing this instance
            role: The role of this instance (LEADER or FOLLOWER)
            address: The network address of this instance
            on_hot_reload: Callback for handling hot reloads
        """
        super().__init__(role, address)
        self.cluster = cluster
        self._instance_loop = instance_loop
        self._on_hot_reload = on_hot_reload
        self._thread_lock = threading.Lock()
        self._initialization_complete = False
        self._sync_handler = None
        
        # Message and buffer handling callbacks
        self._message_handler_callback = None
        self._buffer_handler_callback = None
        self._tick_callback = None
        
        # Hot reload support
        self._check_for_hot_reload()
    
    def initialize(self) -> None:
        """Initialize the instance after UDP handlers are set up."""
        if EnvVars.get_hot_reload():
            logger.info("Hot reload enabled, sending signal")
            self._handle_hot_reload_initialization()
        else:
            logger.info("Starting instance initialization")
            asyncio.get_event_loop().create_task(self._start_instance_initialization())
            
    def _check_for_hot_reload(self) -> None:
        """Check if hot reload is enabled and handle it.
        This is called during initialization, but defers the actual
        announcement until UDP handlers are set up."""
        # Don't do anything here, initialization is deferred
    
    def _handle_hot_reload_initialization(self) -> None:
        """Initialize the hot reload process."""
        # Implementation will be added in the hot reload refactoring
        # For now, just start the normal initialization process
        asyncio.create_task(self._start_instance_initialization())
    
    async def _start_instance_initialization(self) -> None:
        """
        Start the instance initialization process using the selected strategy.
        
        This method uses the registration strategy from the cluster to discover
        other instances and complete initialization.
        """
        registration_mode = EnvVars.get_registration_mode()
        logger.info(f"Starting instance initialization with {registration_mode} registration mode")
        
        # Using the cluster's discovery method to find other instances
        discovery_successful = await self.cluster.discover_instances()
        
        # If using UDP broadcast, also send announcements to help with discovery
        if registration_mode == "broadcast":
            self._send_announce()
        
        # Check if all instances were found immediately
        if discovery_successful and self.cluster.all_accounted_for():
            self._complete_initialization()
        else:
            # If not all instances were found, wait for announcements or retry discovery
            if registration_mode == "broadcast":
                logger.info("Not all instances found, waiting for announcements")
            else:
                logger.warning(f"Not all instances found with {registration_mode} strategy")
                logger.info("Will retry discovery in 5 seconds")
                await asyncio.sleep(5)
                asyncio.create_task(self._start_instance_initialization())
    
    def _send_announce(self) -> None:
        """Send an announcement message to the cluster."""
        all_accounted_for = self.cluster.all_accounted_for()
        
        announce = ClusterAnnounceInstance()
        announce.header.type = ClusterMessageType.ANNOUNCE
        announce.role = self.role
        announce.all_accounted_for = all_accounted_for
        announce.direct_listening_port = EnvVars.get_direct_listen_port()

        # Use instance_address in log message
        logger.info(
            "Announcing instance '%s' (role=%s, all_accounted_for=%s, known_instances=%d/%d, address=%s)",
            EnvVars.get_instance_index(),
            self.role,
            all_accounted_for,
            len(self.cluster.instances),
            self.cluster.instance_count - 1,
            EnvVars.get_instance_address()  # Log the instance address
        )
        self.cluster.udp_message_handler.send_no_wait(announce)
    
    def _register_instance(
        self,
        role: ClusterRole,
        instance_id: int,
        instance_addr: str,
        direct_listening_port: int,
        all_accounted_for: bool,
    ) -> OtherInstance:
        """
        Register another instance in the cluster.
        
        Args:
            role: The role of the instance
            instance_id: The unique ID of the instance
            instance_addr: The network address of the instance
            direct_listening_port: The direct communication port of the instance
            all_accounted_for: Whether this instance has all other instances registered
            
        Returns:
            The registered instance
        """
        other_instance = None

        if role == ClusterRole.LEADER:
            other_instance = OtherLeaderInstance(
                role, instance_addr, direct_listening_port, instance_id
            )
        else:
            other_instance = OtherFollowerInstance(
                role, instance_addr, direct_listening_port, instance_id
            )

        other_instance.all_accounted_for = all_accounted_for
        self.cluster.instances[instance_id] = other_instance
        return other_instance
    
    def _complete_initialization(self) -> None:
        """Mark initialization as complete and update instance addresses."""
        if self._initialization_complete:
            return
            
        logger.info("Initialization complete - all instances connected")
        self._initialization_complete = True
        self.cluster.mark_registration_complete()
        
        # Make sure the UDP system has all instance addresses
        from .udp.udp_base import UDPSingleton
        
        # Rebuild the complete address list to ensure consistency
        addresses = []
        
        # Add this instance
        addresses.append(
            (EnvVars.get_instance_index(), 
             EnvVars.get_instance_address(),  # Use instance_address instead of listen_address
             EnvVars.get_direct_listen_port())
        )
        
        # Add all other registered instances
        addresses.extend(
            (instance_id, instance.address, instance.direct_port)
            for instance_id, instance in self.cluster.instances.items()
        )
        
        # Sort by instance ID for consistency
        addresses.sort(key=lambda x: x[0])
        
        # Log the final address list for debugging
        logger.info("Final cluster instance addresses: %s", addresses)
        
        # Update the UDP singleton with all addresses
        UDPSingleton.set_cluster_instance_addresses(addresses)
        
        # Send final announcement with all_accounted_for=True to help other instances complete
        # This helps ensure all instances transition to the IDLE state together
        if EnvVars.get_udp_broadcast():
            # Force an immediate announcement with all_accounted_for=True
            announce = ClusterAnnounceInstance()
            announce.header.type = ClusterMessageType.ANNOUNCE
            announce.role = self.role
            announce.all_accounted_for = True
            announce.direct_listening_port = EnvVars.get_direct_listen_port()
            
            logger.info("Sending final announcement (initialization complete)")
            self.cluster.udp_message_handler.send_no_wait(announce)
            
            # Send it twice to reduce chance of packet loss
            self.cluster.udp_message_handler.send_no_wait(announce)
    
    def buffer_queue_empty(self) -> bool:
        """Check if the buffer queue is empty."""
        return self._instance_loop.buffer_queue_empty()
    
    async def handle_message(self, incoming_message: IncomingMessage) -> None:
        """
        Handle an incoming message.
        
        Args:
            incoming_message: The incoming message to handle
        """
        # Handle initialization messages
        if not self._initialization_complete:
            if incoming_message.msg_type == ClusterMessageType.ANNOUNCE:
                await self._handle_announce_message(incoming_message)
                return
                
            # Ignore other messages during initialization
            return
        
        # Handle distribute prompt messages
        if incoming_message.msg_type == ClusterMessageType.DISTRIBUTE_PROMPT:
            await self._handle_distribute_prompt(incoming_message)
            return
            
        # Delegate to sync handler if available
        if self._message_handler_callback:
            await self._message_handler_callback(None, incoming_message)
    
    async def _handle_announce_message(self, incoming_message: IncomingMessage) -> None:
        """
        Handle an announce message during initialization.
        
        Args:
            incoming_message: The announce message to handle
        """
        announce_instance = ParseDict(
            incoming_message.message, ClusterAnnounceInstance()
        )
        
        # Check if we already know about this instance
        is_new_instance = incoming_message.sender_instance_id not in self.cluster.instances
        
        if is_new_instance:
            # Register the new instance
            logger.info(
                "New cluster instance '%s' discovered at %s",
                incoming_message.sender_instance_id,
                incoming_message.sender_addr,
            )
            
            other_instance = self._register_instance(
                announce_instance.role,
                incoming_message.sender_instance_id,
                incoming_message.sender_addr,
                announce_instance.direct_listening_port,
                announce_instance.all_accounted_for,
            )
        else:
            # Update the existing instance
            other_instance = self.cluster.instances[incoming_message.sender_instance_id]
            other_instance.all_accounted_for = announce_instance.all_accounted_for
            
        # Always update UDP addresses after receiving an announcement
        from .udp.udp_base import UDPSingleton
        
        # Get current addresses and add new instance
        addresses = []
        
        # Add this instance first
        addresses.append(
            (EnvVars.get_instance_index(), 
             EnvVars.get_instance_address(),  # Use instance_address instead of listen_address
             EnvVars.get_direct_listen_port())
        )
        
        # Add all other instances
        addresses.extend(
            (instance_id, instance.address, instance.direct_port)
            for instance_id, instance in self.cluster.instances.items()
        )
        
        # Sort by instance ID for consistency
        addresses.sort(key=lambda x: x[0])
        
        # Update the UDP singleton with all addresses
        UDPSingleton.set_cluster_instance_addresses(addresses)
        
        # Log current state
        logger.debug(
            "After announcement: have %d/%d instances, all_accounted_for=%s", 
            len(self.cluster.instances), 
            self.cluster.instance_count - 1,
            self.cluster.all_accounted_for()
        )
        
        # Check if all instances are accounted for
        if self.cluster.all_accounted_for():
            # If the other instance says it has all instances, or if we just found the last one
            if announce_instance.all_accounted_for or is_new_instance:
                # If we have all instances, mark as complete immediately
                logger.info("All cluster instances discovered, completing initialization")
                self._complete_initialization()
                
            # If we need to wait for final confirmation, this is handled in handle_state
    
    async def _handle_distribute_prompt(self, incoming_message: IncomingMessage) -> None:
        """
        Handle a distribute prompt message.
        
        Args:
            incoming_message: The distribute prompt message to handle
        """
        distribute_prompt = ParseDict(
            incoming_message.message, ClusterDistributePrompt()
        )

        prompt_json = json.loads(distribute_prompt.prompt)
        json_data = {
            "prompt": prompt_json["output"],
            "extra_data": {"extra_pnginfo": prompt_json["workflow"]},
            "client_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }

        url = f"http://localhost:{EnvVars.get_comfy_port()}/prompt"
        try:
            import requests
            response = requests.post(url, json=json_data)
            response.raise_for_status()
            logger.info("Successfully posted prompt to local ComfyUI instance")
        except Exception as e:
            logger.error(f"Error posting prompt: {str(e)}")
    
    async def handle_buffer(self, incoming_buffer: IncomingBuffer) -> None:
        """
        Handle an incoming buffer.
        
        Args:
            incoming_buffer: The incoming buffer to handle
        """
        # Skip buffer handling during initialization
        if not self._initialization_complete:
            return
            
        # Delegate to sync handler if available
        if self._buffer_handler_callback:
            await self._buffer_handler_callback(None, incoming_buffer)
    
    async def handle_state(self) -> None:
        """Process periodic state updates."""
        # Skip state handling during initialization
        if not self._initialization_complete:
            # If using UDP broadcast, send announcements periodically
            if EnvVars.get_registration_mode() == "broadcast":
                # Check if we need to keep announcing
                if self.cluster.all_accounted_for():
                    # We found all other instances, but they may not have found us yet
                    # Continue announcing with all_accounted_for=True to help others complete
                    self._send_announce()
                    
                    # Check if all instances have reported being all accounted for
                    all_instances_accounted_for = all(
                        instance.all_accounted_for
                        for instance in self.cluster.instances.values()
                    )
                    
                    if all_instances_accounted_for:
                        # Everyone is connected, mark as complete
                        self._complete_initialization()
                else:
                    # Still searching for other instances
                    self._send_announce()
                    
                # Rate limit announcements
                await asyncio.sleep(2)
            
            # For other registration modes, we rely on the cluster's discover_instances method
            return
            
        # Delegate to sync handler if available
        if self._tick_callback:
            await self._tick_callback()
    
    def _register_sync_callbacks(
        self, 
        message_callback: Callable, 
        buffer_callback: Callable, 
        tick_callback: Callable
    ) -> None:
        """
        Register callbacks for synchronization.
        
        Args:
            message_callback: Callback for handling messages
            buffer_callback: Callback for handling buffers
            tick_callback: Callback for periodic processing
        """
        self._message_handler_callback = message_callback
        self._buffer_handler_callback = buffer_callback
        self._tick_callback = tick_callback
    
    def _clear_sync_callbacks(self) -> None:
        """Clear all synchronization callbacks."""
        self._message_handler_callback = None
        self._buffer_handler_callback = None
        self._tick_callback = None
    
    async def distribute_prompt(self, prompt_json: Dict) -> None:
        """
        Distribute a prompt to all instances in the cluster.
        
        Args:
            prompt_json: The prompt to distribute
        """
        logger.info("Distributing prompt to cluster")
        
        message = ClusterDistributePrompt()
        message.header.type = ClusterMessageType.DISTRIBUTE_PROMPT
        message.header.require_ack = True
        message.prompt = json.dumps(prompt_json)
        await self.cluster.udp_message_handler.send_and_wait_thread_safe(message)
    
    async def broadcast_tensor(self, tensor: Any) -> Any:
        """
        Broadcast a tensor to all instances in the cluster.
        
        Args:
            tensor: The tensor to broadcast
            
        Returns:
            The broadcast tensor
        """
        # Lazy load to avoid circular imports
        if not hasattr(self, '_sync_handler') or self._sync_handler is None:
            from .states.sync_state import SyncStateHandler
            self._sync_handler = SyncStateHandler(self)
            
        return await self._sync_handler.begin_tensor_broadcast(tensor)
    
    async def send_tensor_to_leader(self, tensor: Any) -> Any:
        """
        Send a tensor to the leader instance.
        
        Args:
            tensor: The tensor to send
            
        Returns:
            The sent tensor
        """
        # Lazy load to avoid circular imports
        if not hasattr(self, '_sync_handler') or self._sync_handler is None:
            from .states.sync_state import SyncStateHandler
            self._sync_handler = SyncStateHandler(self)
            
        return await self._sync_handler.begin_sender(tensor, [0])
    
    async def receive_tensor_fanin(self, tensor: Any) -> Any:
        """
        Receive a tensor from a fan-in operation.
        
        Args:
            tensor: The tensor to combine with received tensor
            
        Returns:
            The combined tensor
        """
        # Lazy load to avoid circular imports
        if not hasattr(self, '_sync_handler') or self._sync_handler is None:
            from .states.sync_state import SyncStateHandler
            self._sync_handler = SyncStateHandler(self)
            
        return await self._sync_handler.begin_fanin_receiver(tensor)
    
    async def fanout_tensor(self, tensor: Any) -> Any:
        """
        Fan out a tensor to all instances in the cluster.
        
        Args:
            tensor: The tensor to fan out
            
        Returns:
            The portion of the tensor for this instance
        """
        # Lazy load to avoid circular imports
        if not hasattr(self, '_sync_handler') or self._sync_handler is None:
            from .states.sync_state import SyncStateHandler
            self._sync_handler = SyncStateHandler(self)
            
        return await self._sync_handler.begin_fanout_emitter(tensor)
    
    async def receive_tensor_fanout(self) -> Any:
        """
        Receive a tensor from a fan-out operation.
        
        Returns:
            The received tensor
        """
        # Lazy load to avoid circular imports
        if not hasattr(self, '_sync_handler') or self._sync_handler is None:
            from .states.sync_state import SyncStateHandler
            self._sync_handler = SyncStateHandler(self)
            
        return await self._sync_handler.begin_fanout_receiver()
    
    async def gather_tensors(self, tensor: Any) -> Any:
        """
        Gather tensors from all instances in the cluster.
        
        Args:
            tensor: The tensor to gather
            
        Returns:
            The gathered tensors
        """
        # Lazy load to avoid circular imports
        if not hasattr(self, '_sync_handler') or self._sync_handler is None:
            from .states.sync_state import SyncStateHandler
            self._sync_handler = SyncStateHandler(self)
            
        return await self._sync_handler.begin_gathering_tensors(tensor)


class ThisLeaderInstance(ThisInstance):
    """Represents this instance as a leader in the cluster."""
    
    def __init__(
        self,
        cluster: "Cluster",
        instance_loop: "InstanceLoop",
        role: ClusterRole,
        address: str,
        on_hot_reload: Callable[[], None],
    ):
        """
        Initialize this leader instance.
        
        Args:
            cluster: The cluster this instance belongs to
            instance_loop: The instance loop managing this instance
            role: The role of this instance (LEADER)
            address: The network address of this instance
            on_hot_reload: Callback for handling hot reloads
        """
        super().__init__(cluster, instance_loop, role, address, on_hot_reload)


class ThisFollowerInstance(ThisInstance):
    """Represents this instance as a follower in the cluster."""
    
    def __init__(
        self,
        cluster: "Cluster",
        instance_loop: "InstanceLoop",
        role: ClusterRole,
        address: str,
        on_hot_reload: Callable[[], None],
    ):
        """
        Initialize this follower instance.
        
        Args:
            cluster: The cluster this instance belongs to
            instance_loop: The instance loop managing this instance
            role: The role of this instance (FOLLOWER)
            address: The network address of this instance
            on_hot_reload: Callback for handling hot reloads
        """
        super().__init__(cluster, instance_loop, role, address, on_hot_reload)


class OtherLeaderInstance(OtherInstance):
    """Represents another leader instance in the cluster."""
    
    def __init__(
        self,
        role: ClusterRole,
        address: str,
        direct_port: int,
        instance_id: int,
    ):
        """
        Initialize another leader instance.
        
        Args:
            role: The role of this instance (LEADER)
            address: The network address of this instance
            direct_port: The direct communication port of this instance
            instance_id: The unique ID of this instance
        """
        super().__init__(role, address, direct_port, instance_id)


class OtherFollowerInstance(OtherInstance):
    """Represents another follower instance in the cluster."""
    
    def __init__(
        self,
        role: ClusterRole,
        address: str,
        direct_port: int,
        instance_id: int,
    ):
        """
        Initialize another follower instance.
        
        Args:
            role: The role of this instance (FOLLOWER)
            address: The network address of this instance
            direct_port: The direct communication port of this instance
            instance_id: The unique ID of this instance
        """
        super().__init__(role, address, direct_port, instance_id)