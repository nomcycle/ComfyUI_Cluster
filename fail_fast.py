"""
Fail-fast module - Implements validation and early failure detection mechanisms.

This module provides utilities to catch problems early and fail with clear error messages
rather than allowing invalid operations to proceed and cause problems later.
"""
import asyncio
import functools
import inspect
import time
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, cast

from .log import logger
from .env_vars import EnvVars

# Type variables for generic function decorators
F = TypeVar('F', bound=Callable[..., Any])
AsyncF = TypeVar('AsyncF', bound=Callable[..., Any])


def validate_initialization_complete(instance_method: F) -> F:
    """
    Decorator to ensure that instance initialization is complete before proceeding.
    
    Args:
        instance_method: The method to decorate
        
    Returns:
        The decorated method
        
    Raises:
        RuntimeError: If the instance initialization is not complete
    """
    @functools.wraps(instance_method)
    def wrapper(self, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(self, '_initialization_complete') or not self._initialization_complete:
            method_name = instance_method.__name__
            class_name = self.__class__.__name__
            raise RuntimeError(
                f"Cannot call {class_name}.{method_name} before initialization is complete"
            )
        return instance_method(self, *args, **kwargs)
    
    return cast(F, wrapper)


def validate_tensor_operation(tensor_method: AsyncF) -> AsyncF:
    """
    Decorator to validate tensor operations before proceeding.
    
    This ensures that:
    1. Instance initialization is complete
    2. The cluster is fully formed with the expected number of instances
    
    Args:
        tensor_method: The async method to decorate
        
    Returns:
        The decorated async method
        
    Raises:
        RuntimeError: If the tensor operation is not valid
    """
    @functools.wraps(tensor_method)
    async def wrapper(self, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(self, '_initialization_complete') or not self._initialization_complete:
            method_name = tensor_method.__name__
            class_name = self.__class__.__name__
            raise RuntimeError(
                f"Cannot perform tensor operation {class_name}.{method_name} before initialization is complete"
            )
        
        # Check for expected instance count
        if not self.cluster.all_accounted_for():
            method_name = tensor_method.__name__
            class_name = self.__class__.__name__
            expected = self.cluster.instance_count
            actual = len(self.cluster.instances) + 1  # +1 for this instance
            raise RuntimeError(
                f"Cannot perform tensor operation {class_name}.{method_name}: " + 
                f"Expected {expected} instances, but only {actual} are connected"
            )
        
        # All validations passed, proceed with operation
        return await tensor_method(self, *args, **kwargs)
    
    return cast(AsyncF, wrapper)


def with_timeout(timeout_seconds: int) -> Callable[[AsyncF], AsyncF]:
    """
    Decorator to add a timeout to an async method.
    
    Args:
        timeout_seconds: The timeout in seconds
        
    Returns:
        A decorator function
        
    Raises:
        asyncio.TimeoutError: If the method takes longer than the timeout
    """
    def decorator(async_method: AsyncF) -> AsyncF:
        @functools.wraps(async_method)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.wait_for(
                async_method(*args, **kwargs),
                timeout=timeout_seconds
            )
        return cast(AsyncF, wrapper)
    return decorator


class ValidationError(Exception):
    """Exception raised when validation fails."""
    pass


def validate_registration_config() -> None:
    """
    Validate registration configuration based on the selected mode.
    
    Raises:
        ValidationError: If the configuration is invalid
    """
    mode = EnvVars.get_registration_mode()
    
    # Validate STUN mode configuration
    if mode == "stun":
        stun_url = EnvVars.get_stun_server_url()
        if not stun_url:
            raise ValidationError(
                "STUN server URL not configured. Set COMFY_CLUSTER_STUN_SERVER environment variable."
            )
    
    # Validate static mode configuration
    elif mode == "static":
        hostnames = EnvVars.get_udp_hostnames()
        if not hostnames:
            raise ValidationError(
                "Static hostnames not configured. Set COMFY_CLUSTER_UDP_HOSTNAMES environment variable."
            )
        
        # Validate that all hostnames are well-formed
        for instance_id, hostname, port in hostnames:
            if not isinstance(instance_id, int):
                raise ValidationError(f"Invalid instance ID in hostnames: {instance_id}")
            if not isinstance(hostname, str) or not hostname:
                raise ValidationError(f"Invalid hostname in hostnames: {hostname}")
            if not isinstance(port, int) or port <= 0:
                raise ValidationError(f"Invalid port in hostnames: {port}")
    
    # Validate instance ID
    instance_id = EnvVars.get_instance_index()
    instance_count = EnvVars.get_instance_count()
    
    if instance_id < 0 or instance_id >= instance_count:
        raise ValidationError(
            f"Instance index {instance_id} out of range. Must be between 0 and {instance_count-1}."
        )
    
    # Validate role
    role = EnvVars.get_instance_role()
    if instance_id == 0 and str(role) != "LEADER":
        raise ValidationError(
            f"Instance 0 must be LEADER, but role is {role}"
        )


class DiscoveryStatus:
    """
    Tracks the status of instance discovery.
    
    This class provides a way to monitor discovery attempts and trigger alerts
    if discovery is not progressing.
    """
    
    def __init__(
        self, 
        retry_limit: int = 10, 
        alert_threshold: int = 3,
        warning_interval: int = 5
    ):
        """
        Initialize the discovery status tracker.
        
        Args:
            retry_limit: Maximum number of retries before giving up
            alert_threshold: Number of retries before logging a warning
            warning_interval: How often to log warnings (in retry attempts)
        """
        self.attempts = 0
        self.last_instance_count = 0
        self.retry_limit = retry_limit
        self.alert_threshold = alert_threshold
        self.warning_interval = warning_interval
        self.start_time = time.time()
    
    def update(self, current_instance_count: int) -> None:
        """
        Update the discovery status.
        
        Args:
            current_instance_count: The current number of discovered instances
        """
        self.attempts += 1
        progress = current_instance_count > self.last_instance_count
        self.last_instance_count = current_instance_count
        
        # Log discovery progress
        elapsed = time.time() - self.start_time
        
        if self.attempts >= self.alert_threshold and not progress:
            # Only log if this is an interval or the first alert
            if self.attempts == self.alert_threshold or self.attempts % self.warning_interval == 0:
                logger.warning(
                    f"Discovery not progressing after {self.attempts} attempts "
                    f"({elapsed:.1f}s elapsed). Still at {current_instance_count} instances."
                )
                
                # Provide hints based on registration mode
                mode = EnvVars.get_registration_mode()
                if mode == "stun":
                    logger.warning(
                        "STUN registration hint: Check STUN server URL and network connectivity. "
                        "Ensure COMFY_CLUSTER_STUN_SERVER is set correctly on all instances."
                    )
                elif mode == "static":
                    logger.warning(
                        "Static registration hint: Check COMFY_CLUSTER_UDP_HOSTNAMES configuration. "
                        "Ensure all hostnames and ports are correct and instances are running."
                    )
                elif mode == "broadcast":
                    logger.warning(
                        "Broadcast registration hint: Check network broadcast settings. "
                        "Ensure instances are on the same network and can receive UDP broadcasts."
                    )
    
    def check_limit_exceeded(self) -> bool:
        """
        Check if the retry limit has been exceeded.
        
        Returns:
            True if the retry limit has been exceeded, False otherwise
        """
        return self.attempts >= self.retry_limit