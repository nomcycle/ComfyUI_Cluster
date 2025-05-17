#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Authentication Module

This module handles authentication for the STUN server.
Implements strict fail-fast authentication with no recovery paths.
"""

import logging
from typing import Dict, Optional, Callable, Protocol, runtime_checkable

from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader

from .exceptions import ConfigurationError, AuthenticationError

logger = logging.getLogger("comfyui-cluster-stun.auth")

# Constants
CLUSTER_KEY_HEADER = "X-Cluster-Key"

@runtime_checkable
class APIKeyProvider(Protocol):
    """Protocol for API key providers."""
    
    async def __call__(self) -> Optional[str]:
        """Get the API key."""
        ...

class HeaderAPIKeyProvider:
    """Provides API keys from HTTP headers."""
    
    def __init__(self, header_name: str = CLUSTER_KEY_HEADER):
        """
        Initialize with the header name.
        
        Args:
            header_name: Name of the API key header
        """
        self.api_key_header = APIKeyHeader(name=header_name, auto_error=False)
    
    async def __call__(self) -> Optional[str]:
        """
        Get the API key from the request header.
        
        Returns:
            The API key or None if not provided
        """
        return await self.api_key_header()

def validate_cluster_id(cluster_id: Optional[str]) -> str:
    """
    Validate that a cluster ID is provided.
    
    Args:
        cluster_id: Cluster ID to validate
        
    Returns:
        Validated cluster ID
        
    Raises:
        HTTPException: If cluster ID is invalid
    """
    if not cluster_id:
        logger.warning("Missing cluster ID in authentication request")
        raise HTTPException(
            status_code=400,
            detail="Cluster ID is required"
        )
    return cluster_id

def validate_api_key(api_key: Optional[str], cluster_id: str) -> str:
    """
    Validate that an API key is provided.
    
    Args:
        api_key: API key to validate
        cluster_id: Associated cluster ID for logging
        
    Returns:
        Validated API key
        
    Raises:
        HTTPException: If API key is invalid
    """
    if not api_key:
        logger.warning(f"Missing cluster key in authentication request for cluster: {cluster_id}")
        raise HTTPException(
            status_code=401,
            detail="Cluster key is required",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return api_key

class KeyBasedAuthProvider:
    """
    Authentication provider using predefined API keys.
    Strictly follows fail-fast principles.
    """
    def __init__(
        self, 
        cluster_keys: Dict[str, str],
        api_key_provider: Optional[APIKeyProvider] = None
    ):
        """
        Initialize the auth provider with predefined keys.
        
        Args:
            cluster_keys: Dictionary mapping cluster IDs to their authentication keys
            api_key_provider: Provider for API keys (defaults to header-based)
            
        Raises:
            ConfigurationError: If no cluster keys are provided
        """
        if not cluster_keys:
            msg = "No cluster keys provided for authentication"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        self.cluster_keys = cluster_keys
        self.api_key_provider = api_key_provider or HeaderAPIKeyProvider()
        logger.info(f"Initialized key-based authentication for {len(cluster_keys)} clusters")
    
    def check_cluster_configured(self, cluster_id: str) -> bool:
        """
        Check if a cluster ID is configured.
        
        Args:
            cluster_id: The ID of the cluster
            
        Returns:
            True if the cluster is configured
            
        Raises:
            HTTPException: If the cluster is not configured
        """
        if cluster_id not in self.cluster_keys:
            logger.warning(f"Attempt to access unconfigured cluster ID: {cluster_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Cluster ID '{cluster_id}' is not configured on this server"
            )
        return True
    
    def verify_key_matches(self, cluster_id: str, api_key: str) -> bool:
        """
        Verify that the provided API key matches the configured key.
        
        Args:
            cluster_id: The ID of the cluster
            api_key: The provided API key
            
        Returns:
            True if the key matches
            
        Raises:
            HTTPException: If the key doesn't match
        """
        if api_key != self.cluster_keys[cluster_id]:
            logger.warning(f"Invalid cluster key for cluster: {cluster_id}")
            raise HTTPException(
                status_code=403,
                detail="Invalid cluster key",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        return True
    
    def authenticate(self, cluster_id: str, api_key: str) -> bool:
        """
        Authenticate a request using the provided API key.
        
        Args:
            cluster_id: The ID of the cluster
            api_key: The provided API key
            
        Returns:
            True if authentication succeeds
            
        Raises:
            HTTPException: If authentication fails with appropriate status code and detail
        """
        # Validate inputs
        cluster_id = validate_cluster_id(cluster_id)
        api_key = validate_api_key(api_key, cluster_id)
        
        # Check cluster and key
        self.check_cluster_configured(cluster_id)
        self.verify_key_matches(cluster_id, api_key)
        
        # If we get here, authentication succeeded
        return True
    
    async def get_api_key(self) -> Optional[str]:
        """
        Get the API key using the configured provider.
        
        Returns:
            The API key or None if not provided
        """
        return await self.api_key_provider()
    
    def get_security_dependency(self) -> Callable:
        """
        Get a FastAPI dependency for authentication.
        
        Returns:
            A callable that can be used with FastAPI's Depends
        """
        async def authenticate_cluster(
            cluster_id: Optional[str] = None,
            admin_cluster_id: Optional[str] = None,
            api_key: Optional[str] = None
        ) -> bool:
            # Determine which cluster ID to use
            # Path parameters like /clusters/{admin_cluster_id} or /instance/{cluster_id}
            # will be automatically injected
            effective_cluster_id = cluster_id or admin_cluster_id
            effective_cluster_id = validate_cluster_id(effective_cluster_id)
            
            # If api_key is not provided, try to get it from the provider
            if api_key is None:
                api_key = await self.get_api_key()
            
            return self.authenticate(effective_cluster_id, api_key or "")
        
        return authenticate_cluster