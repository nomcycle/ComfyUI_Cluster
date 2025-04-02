"""
Synchronization utilities for UDP networking.
"""
import asyncio
import time
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Callable, Any, TypeVar, Generic

from ..log import logger
from .exceptions import FenceError, StateResolveError, ExpectedMessageError
from .models import InstanceAddress


T = TypeVar('T')


class SyncState(Enum):
    """
    Represents the state of a synchronization operation.
    """
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()
    TIMED_OUT = auto()


class SynchronizationContext:
    """
    Provides context for synchronization operations across cluster instances.
    """
    def __init__(self, 
                 instance_id: int, 
                 instance_addresses: List[InstanceAddress],
                 timeout: float = 30.0):
        self.instance_id = instance_id
        self.instance_addresses = instance_addresses
        self.default_timeout = timeout
        self.instance_ids = {addr.instance_id for addr in instance_addresses 
                            if addr.instance_id != instance_id}
    
    @property
    def instance_count(self) -> int:
        """Get the total number of instances in the cluster."""
        return len(self.instance_addresses)
    
    def get_other_instance_ids(self) -> Set[int]:
        """Get the IDs of all other instances in the cluster."""
        return self.instance_ids
    
    def get_instance_address(self, instance_id: int) -> Optional[InstanceAddress]:
        """Get the address of a specific instance."""
        for addr in self.instance_addresses:
            if addr.instance_id == instance_id:
                return addr
        return None


class SyncOperation(Generic[T], ABC):
    """
    Base class for synchronization operations.
    """
    def __init__(self, 
                 context: SynchronizationContext,
                 operation_id: Any,
                 timeout: Optional[float] = None):
        self.context = context
        self.operation_id = operation_id
        self.timeout = timeout or context.default_timeout
        self.state = SyncState.PENDING
        self.start_time = None
        self.end_time = None
        self.result: Optional[T] = None
        self.error = None
    
    @abstractmethod
    async def execute(self) -> T:
        """Execute the synchronization operation."""
        pass
    
    def _start(self) -> None:
        """Mark the operation as started."""
        self.state = SyncState.IN_PROGRESS
        self.start_time = time.time()
    
    def _complete(self, result: T) -> None:
        """Mark the operation as completed."""
        self.state = SyncState.COMPLETED
        self.end_time = time.time()
        self.result = result
    
    def _fail(self, error: Exception) -> None:
        """Mark the operation as failed."""
        self.state = SyncState.FAILED
        self.end_time = time.time()
        self.error = error
    
    def _timeout(self) -> None:
        """Mark the operation as timed out."""
        self.state = SyncState.TIMED_OUT
        self.end_time = time.time()
    
    @property
    def duration(self) -> Optional[float]:
        """Get the duration of the operation in seconds."""
        if self.start_time is None:
            return None
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    @property
    def is_complete(self) -> bool:
        """Check if the operation is complete."""
        return self.state in {SyncState.COMPLETED, SyncState.FAILED, SyncState.TIMED_OUT}
    
    @property
    def is_successful(self) -> bool:
        """Check if the operation was successful."""
        return self.state == SyncState.COMPLETED


class FenceOperation(SyncOperation[bool]):
    """
    Represents a synchronization fence operation.
    """
    def __init__(self, 
                 context: SynchronizationContext,
                 fence_id: int,
                 send_fence_message_fn: Callable[[int], asyncio.Future],
                 timeout: Optional[float] = None):
        super().__init__(context, fence_id, timeout)
        self.fence_id = fence_id
        self._send_fence_message = send_fence_message_fn
        self._instance_states: Dict[int, bool] = {
            instance_id: False for instance_id in context.get_other_instance_ids()
        }
    
    def mark_instance_reached(self, instance_id: int) -> None:
        """Mark an instance as having reached the fence."""
        if instance_id in self._instance_states:
            self._instance_states[instance_id] = True
            logger.debug(f"Instance {instance_id} reached fence {self.fence_id}")
            
            # Check if all instances have reached the fence
            if all(self._instance_states.values()):
                logger.info(f"All instances reached fence {self.fence_id}")
                self._complete(True)
    
    @property
    def pending_instances(self) -> List[int]:
        """Get the list of instances that haven't reached the fence yet."""
        return [instance_id for instance_id, reached in self._instance_states.items() 
                if not reached]
    
    async def execute(self) -> bool:
        """Execute the fence operation."""
        self._start()
        logger.info(f"Starting fence operation {self.fence_id}")
        
        try:
            # If there are no other instances, complete immediately
            if not self._instance_states:
                self._complete(True)
                return True
            
            # Send fence message to all instances
            fence_future = await self._send_fence_message(self.fence_id)
            
            # Wait for the future to complete or timeout
            try:
                result = await asyncio.wait_for(fence_future, self.timeout)
                self._complete(True)
                return True
            except asyncio.TimeoutError:
                self._timeout()
                pending = self.pending_instances
                error_msg = f"Fence operation {self.fence_id} timed out after {self.timeout}s, waiting for instances: {pending}"
                logger.error(error_msg)
                raise FenceError(error_msg)
                
        except Exception as e:
            error_msg = f"Fence operation {self.fence_id} failed: {str(e)}"
            logger.error(error_msg)
            self._fail(e)
            raise FenceError(error_msg) from e


class StateResolveOperation(SyncOperation[Dict[int, Any]]):
    """
    Represents a state resolution operation.
    """
    def __init__(self, 
                 context: SynchronizationContext,
                 state_id: Any,
                 state_type: Any,
                 request_state_fn: Callable[[Any], asyncio.Future],
                 timeout: Optional[float] = None):
        super().__init__(context, state_id, timeout)
        self.state_id = state_id
        self.state_type = state_type
        self._request_state = request_state_fn
        self._instance_states: Dict[int, Any] = {}
    
    def resolve_instance_state(self, instance_id: int, state: Any) -> None:
        """Record the state from an instance."""
        self._instance_states[instance_id] = state
        logger.debug(f"Received state from instance {instance_id} for {self.state_id}")
        
        # Check if we've received states from all instances
        if all(instance_id in self._instance_states for instance_id in self.context.get_other_instance_ids()):
            logger.info(f"All instances resolved state for {self.state_id}")
            self._complete(self._instance_states.copy())
    
    @property
    def pending_instances(self) -> List[int]:
        """Get the list of instances that haven't provided their state yet."""
        return [instance_id for instance_id in self.context.get_other_instance_ids() 
                if instance_id not in self._instance_states]
    
    async def execute(self) -> Dict[int, Any]:
        """Execute the state resolution operation."""
        self._start()
        logger.info(f"Starting state resolution operation {self.state_id}")
        
        try:
            # If there are no other instances, complete immediately
            if not self.context.get_other_instance_ids():
                self._complete({})
                return {}
            
            # Send state request to all instances
            state_future = await self._request_state(self.state_type)
            
            # Wait for the future to complete or timeout
            try:
                result = await asyncio.wait_for(state_future, self.timeout)
                if not self.is_complete:  # May already be complete if all responses arrived
                    self._complete(self._instance_states.copy())
                return self._instance_states
            except asyncio.TimeoutError:
                self._timeout()
                pending = self.pending_instances
                error_msg = f"State resolve operation {self.state_id} timed out after {self.timeout}s, waiting for instances: {pending}"
                logger.error(error_msg)
                raise StateResolveError(error_msg)
                
        except Exception as e:
            error_msg = f"State resolve operation {self.state_id} failed: {str(e)}"
            logger.error(error_msg)
            self._fail(e)
            raise StateResolveError(error_msg) from e


class SynchronizationManager:
    """
    Manages synchronization operations across cluster instances.
    """
    def __init__(self, context: SynchronizationContext):
        self.context = context
        self._active_fences: Dict[int, FenceOperation] = {}
        self._active_state_resolves: Dict[Any, StateResolveOperation] = {}
        self._fence_send_fn: Optional[Callable[[int], asyncio.Future]] = None
        self._state_request_fn: Optional[Callable[[Any], asyncio.Future]] = None
    
    def register_fence_sender(self, send_fn: Callable[[int], asyncio.Future]) -> None:
        """Register a function for sending fence messages."""
        self._fence_send_fn = send_fn
    
    def register_state_requester(self, request_fn: Callable[[Any], asyncio.Future]) -> None:
        """Register a function for requesting state from other instances."""
        self._state_request_fn = request_fn
    
    async def create_fence(self, fence_id: int, timeout: Optional[float] = None) -> bool:
        """Create a new synchronization fence."""
        if fence_id in self._active_fences:
            raise FenceError(f"Fence {fence_id} already exists")
        
        if not self._fence_send_fn:
            raise FenceError("No fence sender function registered")
        
        fence = FenceOperation(
            self.context,
            fence_id,
            self._fence_send_fn,
            timeout
        )
        self._active_fences[fence_id] = fence
        result = await fence.execute()
        return result
    
    def handle_fence_message(self, fence_id: int, sender_instance_id: int) -> None:
        """Handle a fence message from another instance."""
        fence = self._active_fences.get(fence_id)
        if fence:
            fence.mark_instance_reached(sender_instance_id)
        else:
            # Create a placeholder fence to track instances that have reported
            logger.debug(f"Creating placeholder fence for ID {fence_id}")
            fence = FenceOperation(
                self.context,
                fence_id,
                lambda _: asyncio.Future(),  # Dummy function
                self.context.default_timeout
            )
            self._active_fences[fence_id] = fence
            fence.mark_instance_reached(sender_instance_id)
    
    async def resolve_state(self, state_id: Any, state_type: Any, timeout: Optional[float] = None) -> Dict[int, Any]:
        """Resolve state across all instances."""
        if state_id in self._active_state_resolves:
            raise StateResolveError(f"State resolve {state_id} already exists")
        
        if not self._state_request_fn:
            raise StateResolveError("No state request function registered")
        
        resolver = StateResolveOperation(
            self.context,
            state_id,
            state_type,
            self._state_request_fn,
            timeout
        )
        self._active_state_resolves[state_id] = resolver
        result = await resolver.execute()
        return result
    
    def handle_state_response(self, state_id: Any, sender_instance_id: int, state: Any) -> None:
        """Handle a state response from another instance."""
        resolver = self._active_state_resolves.get(state_id)
        if resolver:
            resolver.resolve_instance_state(sender_instance_id, state)
        else:
            logger.warning(f"Received state response for unknown state ID {state_id} from instance {sender_instance_id}")
    
    def cleanup(self) -> None:
        """Clean up completed operations."""
        # Clean up completed fence operations
        for fence_id in list(self._active_fences.keys()):
            fence = self._active_fences[fence_id]
            if fence.is_complete:
                del self._active_fences[fence_id]
        
        # Clean up completed state resolution operations
        for state_id in list(self._active_state_resolves.keys()):
            resolver = self._active_state_resolves[state_id]
            if resolver.is_complete:
                del self._active_state_resolves[state_id]