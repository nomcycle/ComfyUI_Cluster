#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Authentication Module

This module handles authentication for the STUN server.
Implements strict fail-fast authentication with no recovery paths.
"""

import logging
from typing import Dict, Optional, Callable

from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader

from .exceptions import ConfigurationError, AuthenticationError

logger = logging.getLogger("comfyui-cluster-stun.auth")

# Constants
CLUSTER_KEY_HEADER = "X-Cluster-Key"


class KeyBasedAuthProvider:
    """
    Authentication provider using predefined API keys.
    Strictly follows fail-fast principles.
    """
    def __init__(self, cluster_keys: Dict[str, str]):
        """
        Initialize the auth provider with predefined keys.
        
        Args:
            cluster_keys: Dictionary mapping cluster IDs to their authentication keys
            
        Raises:
            ConfigurationError: If no cluster keys are provided
        """
        if not cluster_keys:
            msg = "No cluster keys provided for authentication"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        self.cluster_keys = cluster_keys
        self.api_key_header = APIKeyHeader(name=CLUSTER_KEY_HEADER, auto_error=False)
        logger.info(f"Initialized key-based authentication for {len(cluster_keys)} clusters")
    
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
        # Validate cluster ID
        if not cluster_id:
            logger.warning("Missing cluster ID in authentication request")
            raise HTTPException(
                status_code=400,
                detail="Cluster ID is required"
            )
        
        # Validate API key presence
        if not api_key:
            logger.warning(f"Missing cluster key in authentication request for cluster: {cluster_id}")
            raise HTTPException(
                status_code=401,
                detail="Cluster key is required",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        
        # Check if this cluster ID is configured
        if cluster_id not in self.cluster_keys:
            logger.warning(f"Attempt to access unconfigured cluster ID: {cluster_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Cluster ID '{cluster_id}' is not configured on this server"
            )
        
        # Check if the key matches
        if api_key != self.cluster_keys[cluster_id]:
            logger.warning(f"Invalid cluster key for cluster: {cluster_id}")
            raise HTTPException(
                status_code=403,
                detail="Invalid cluster key",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        
        # If we get here, authentication succeeded
        return True
    
    async def get_api_key(self) -> Optional[str]:
        """
        Get the API key from the request header.
        
        Returns:
            The API key or None if not provided
        """
        return await self.api_key_header()
    
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
            
            if not effective_cluster_id:
                logger.error("No cluster ID provided for authentication")
                raise HTTPException(
                    status_code=400,
                    detail="Cluster ID is required"
                )
            
            # If api_key is not provided, try to get it from the header
            if api_key is None:
                api_key = await self.get_api_key()
            
            return self.authenticate(effective_cluster_id, api_key or "")
        
        return authenticate_cluster