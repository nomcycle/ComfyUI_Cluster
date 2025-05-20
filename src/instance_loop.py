"""
Instance loop module - Manages the instance lifecycle and message processing.
"""
import asyncio
import threading
import queue
from typing import Optional

from .log import logger
from .env_vars import EnvVars
from .protobuf.messages_pb2 import ClusterRole
from .cluster import Cluster
from .instance import ThisInstance, ThisLeaderInstance, ThisFollowerInstance
from .udp.udp_base import UDPSingleton
from .udp.udp_handle_message import UDPMessageHandler
from .udp.udp_handle_buffer import UDPBufferHandler

# Global singleton instance (there should only be one per process)
_instance_loop = None


class InstanceLoop:
    """
    Manages the instance lifecycle and message processing.
    
    This class runs two threads:
    1. State thread - Handles the instance state and periodic processing
    2. Packet thread - Processes incoming messages and buffers
    
    It's implemented as a singleton to ensure only one instance exists.
    """
    
    def __init__(self) -> None:
        """Initialize the instance loop and start the processing threads."""
        global _instance_loop
        if _instance_loop is not None:
            raise RuntimeError("InstanceLoop is a singleton - use get_instance()")

        # Instance role and state
        self._instance_role = EnvVars.get_instance_role()
        self._running = True
        self._this_instance: Optional[ThisInstance] = None
        self._cluster: Optional[Cluster] = None
        
        # Thread synchronization
        self._state_lock = threading.Lock()
        
        # Message and buffer queues
        self._incoming_message_queue = queue.Queue(maxsize=1000)
        self._incoming_buffer_queue = queue.Queue(maxsize=1000000000)
        
        # Create and start threads
        self._state_thread = threading.Thread(
            target=self._run_state_loop, 
            daemon=True,
            name="ComfyCluster-StateHandler"
        )
        
        self._packet_thread = threading.Thread(
            target=self._run_packet_loop, 
            daemon=True,
            name="ComfyCluster-MessageHandler"
        )
        
        # Start threads
        self._state_thread.start()
        self._packet_thread.start()

    @property
    def instance(self) -> ThisInstance:
        """Get the instance being managed by this loop."""
        if self._this_instance is None:
            raise RuntimeError("Instance not initialized")
        return self._this_instance

    def buffer_queue_empty(self) -> bool:
        """Check if the buffer queue is empty."""
        return self._incoming_buffer_queue.empty()

    def _on_hot_reload(self) -> None:
        """Clean up resources for hot reload."""
        logger.info("Cleaning up for hot reload...")

        # Stop threads
        self._running = False
        
        # Cancel pending messages
        if self._this_instance and self._this_instance.cluster:
            self._this_instance.cluster.udp_message_handler.cancel_all_pending()

        # Stop UDP threads
        UDPSingleton.stop_threads()
        
        # Clean up references
        if self._this_instance:
            if hasattr(self._this_instance, 'cluster'):
                if hasattr(self._this_instance.cluster, 'udp_buffer_handler'):
                    del self._this_instance.cluster.udp_buffer_handler
                if hasattr(self._this_instance.cluster, 'udp_message_handler'):
                    del self._this_instance.cluster.udp_message_handler
                del self._this_instance.cluster
            del self._this_instance

        # Clear the global instance
        global _instance_loop
        _instance_loop = None
        logger.info("Hot reload cleanup complete")

    def _run_state_loop(self) -> None:
        """Run the state thread that handles instance state and periodic processing."""
        try:
            # Set up asyncio event loop for this thread
            self._state_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._state_loop)

            # Initialize cluster and instance
            self._cluster = Cluster()
            
            # Create the appropriate instance type based on role
            if self._instance_role == ClusterRole.LEADER:
                self._this_instance = ThisLeaderInstance(
                    self._cluster,
                    self,
                    ClusterRole.LEADER,
                    "localhost",
                    self._on_hot_reload,
                )
            else:
                self._this_instance = ThisFollowerInstance(
                    self._cluster,
                    self,
                    ClusterRole.FOLLOWER,
                    "localhost",
                    self._on_hot_reload,
                )

            # Initialize UDP handlers
            udp_message_handler = UDPMessageHandler(
                self._state_loop, self._incoming_message_queue
            )
            udp_buffer_handler = UDPBufferHandler(
                self._state_loop, self._incoming_buffer_queue
            )

            # Set up handlers in the cluster
            self._cluster.set_udp_message_handler(udp_message_handler)
            self._cluster.set_udp_buffer_handler(udp_buffer_handler)

            # Start UDP threads
            UDPSingleton.start_threads()
            
            # Now that UDP handlers are set up, initialize the instance
            self._this_instance.initialize()

            # Run the asyncio event loop for state processing
            self._state_loop.run_until_complete(self._state_loop_async())
        except Exception as e:
            logger.error("State loop initialization failed: %s", str(e), exc_info=True)
            raise
        finally:
            # Clean up
            if hasattr(self, '_state_loop'):
                self._state_loop.close()
            logger.info("State loop thread terminated")

    async def _state_loop_async(self) -> None:
        """Handle the asyncio event loop for state processing."""
        try:
            # Process state continuously while running
            while self._running:
                await self._this_instance.handle_state()
                await asyncio.sleep(0.001)
        except Exception as e:
            logger.error("State loop failed: %s", str(e), exc_info=True)
            raise
        finally:
            logger.info("State loop async terminated")

    def _run_packet_loop(self) -> None:
        """Run the packet thread that processes incoming messages and buffers."""
        try:
            # Set up asyncio event loop for this thread
            self._packet_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._packet_loop)
            
            # Run the asyncio event loop for packet processing
            self._packet_loop.run_until_complete(self._packet_loop_async())
        except Exception as e:
            logger.error("Packet loop initialization failed: %s", str(e), exc_info=True)
            raise
        finally:
            # Clean up
            if hasattr(self, '_packet_loop'):
                self._packet_loop.close()
            logger.info("Packet loop thread terminated")

    async def _packet_loop_async(self) -> None:
        """Handle the asyncio event loop for packet processing."""
        try:
            # Process packets continuously while running
            while self._running:
                # Process any incoming buffers (larger data like tensors)
                await self._process_buffer_queue()
                
                # Process any incoming messages (control messages)
                await self._process_message_queue()
                
                # Small sleep to avoid busy-waiting
                await asyncio.sleep(0.001)
        except Exception as e:
            logger.error("Packet loop failed: %s", str(e), exc_info=True)
            raise
        finally:
            logger.info("Packet loop async terminated")
    
    async def _process_buffer_queue(self) -> None:
        """Process all available buffers in the queue."""
        while True:
            try:
                # Get the next buffer without blocking
                incoming_buffer = self._incoming_buffer_queue.get_nowait()
                
                # Process the buffer
                await self._this_instance.handle_buffer(incoming_buffer)
                
                # Mark as done
                self._incoming_buffer_queue.task_done()
            except queue.Empty:
                # No more buffers to process
                break
            except Exception as e:
                logger.error("Error processing buffer: %s", str(e), exc_info=True)
    
    async def _process_message_queue(self) -> None:
        """Process all available messages in the queue."""
        while True:
            try:
                # Get the next message without blocking
                incoming_message = self._incoming_message_queue.get_nowait()
                
                # Process the message
                await self._this_instance.handle_message(incoming_message)
                
                # Mark as done
                self._incoming_message_queue.task_done()
            except queue.Empty:
                # No more messages to process
                break
            except Exception as e:
                logger.error("Error processing message: %s", str(e), exc_info=True)


class LeaderInstanceLoop(InstanceLoop):
    """Specialized instance loop for leader nodes."""
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance."""
        global _instance_loop
        if _instance_loop is None:
            _instance_loop = cls()
        return _instance_loop


class FollowerInstanceLoop(InstanceLoop):
    """Specialized instance loop for follower nodes."""
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance."""
        global _instance_loop
        if _instance_loop is None:
            _instance_loop = cls()
        return _instance_loop


def get_instance_loop():
    """Get the appropriate instance loop based on the instance role."""
    return (
        LeaderInstanceLoop.get_instance()
        if EnvVars.get_instance_role() == ClusterRole.LEADER
        else FollowerInstanceLoop.get_instance()
    )