#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Configuration Module

This module handles configuration loading and validation with fail-fast principles.
"""

import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional, Callable, Any

from .exceptions import ConfigurationError

logger = logging.getLogger("comfyui-cluster-stun.config")

@dataclass
class ServerConfig:
    host: str
    port: int
    log_level: str
    cluster_keys: Dict[str, str]
    
    def __post_init__(self):
        if not self.host:
            msg = "Host cannot be empty"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        if not isinstance(self.port, int) or self.port < 1 or self.port > 65535:
            msg = f"Invalid port number: {self.port}"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        if not self.log_level:
            msg = "Log level cannot be empty"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        if not self.cluster_keys:
            logger.warning("No cluster keys configured. This is highly insecure!")

class EnvReader:
    """
    Environment variable reader that can be mocked for testing.
    Provides a layer of indirection for environment variable access.
    """
    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get an environment variable.
        
        Args:
            name: Name of the environment variable
            default: Default value if not found
            
        Returns:
            Value of the environment variable or default
        """
        return os.getenv(name, default)
            
def load_cluster_keys(env_reader: Optional[EnvReader] = None) -> Dict[str, str]:
    """
    Load predefined cluster keys from environment variables.
    Follows fail-fast principles by validating early and failing explicitly.
    
    Args:
        env_reader: Environment variable reader (for testing)
        
    Returns:
        Dict mapping cluster IDs to their authentication keys
        
    Raises:
        ConfigurationError: If the configuration is invalid
    """
    env_reader = env_reader or EnvReader()
    cluster_keys = {}
    auth_keys_env = env_reader.get("COMFY_CLUSTER_AUTH_KEYS", "")
    
    if not auth_keys_env:
        msg = "No cluster keys configured!"
        logger.error(msg)
        raise ConfigurationError(msg)

    # Parse the environment variable with strict validation
    key_pairs = auth_keys_env.split(",")
    for pair in key_pairs:
        if ":" not in pair:
            msg = f"Invalid key pair format: {pair}"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        cluster_id, key = pair.split(":", 1)
        cluster_id = cluster_id.strip()
        key = key.strip()
        
        if not cluster_id:
            msg = "Empty cluster ID found in configuration"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        if len(key) < 8:
            msg = f"Cluster key for '{cluster_id}' is too short (<8 chars)"
            logger.error(msg)
            raise ConfigurationError(msg)
            
        cluster_keys[cluster_id] = key
        # Log only part of the key for security
        masked_key = key[:3] + "*" * (len(key) - 6) + key[-3:]
        logger.info(f"Loaded auth key for cluster: {cluster_id} ({masked_key})")
    
    return cluster_keys

def get_config_from_env(
    args: Optional[dict] = None, 
    env_reader: Optional[EnvReader] = None
) -> ServerConfig:
    """
    Create configuration from environment variables and command line arguments.
    Command line arguments take precedence over environment variables.
    
    Args:
        args: Optional dictionary of command line arguments
        env_reader: Environment variable reader (for testing)
        
    Returns:
        ServerConfig object with validated configuration
        
    Raises:
        ConfigurationError: If the configuration is invalid
    """
    args = args or {}
    env_reader = env_reader or EnvReader()
    
    # Get values with precedence: args > env > default
    host = args.get("host") or env_reader.get("COMFY_CLUSTER_STUN_HOST", "0.0.0.0")
    
    # Port needs to be an integer
    port_str = args.get("port") or env_reader.get("COMFY_CLUSTER_STUN_PORT", "8089")
    try:
        port = int(port_str)
    except ValueError:
        msg = f"Invalid port number: {port_str}"
        logger.error(msg)
        raise ConfigurationError(msg)
    
    log_level = args.get("log_level") or env_reader.get("COMFY_CLUSTER_STUN_LOG_LEVEL", "INFO")
    
    # Load and validate cluster keys
    cluster_keys = load_cluster_keys(env_reader)
    
    return ServerConfig(
        host=host,
        port=port,
        log_level=log_level,
        cluster_keys=cluster_keys
    )