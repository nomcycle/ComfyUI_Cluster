#!/usr/bin/env python3
"""
Test fixtures for ComfyUI Cluster STUN Server tests.
"""

import pytest
from unittest.mock import MagicMock

from src.config import EnvReader, ServerConfig
from src.exceptions import ConfigurationError


@pytest.fixture
def mock_env_reader():
    """
    Fixture that provides a mock environment reader for testing configuration.
    """
    reader = MagicMock(spec=EnvReader)
    
    # Set up a default implementation for get() method
    env_vars = {}
    
    def mock_get(name, default=None):
        return env_vars.get(name, default)
    
    reader.get.side_effect = mock_get
    
    # Add a helper to set environment variables
    def set_env(name, value):
        env_vars[name] = value
        
    reader.set_env = set_env
    
    # Add a helper to clear environment variables
    def clear_env():
        env_vars.clear()
        
    reader.clear_env = clear_env
    
    return reader


@pytest.fixture
def valid_server_config():
    """
    Fixture that provides a valid server configuration.
    """
    return ServerConfig(
        host="127.0.0.1",
        port=8089,
        log_level="INFO",
        cluster_keys={"test_cluster": "test_key_12345678"}
    )


@pytest.fixture
def mock_env_with_cluster_keys(mock_env_reader):
    """
    Fixture that sets up a mock environment with valid cluster keys.
    """
    mock_env_reader.set_env("COMFY_CLUSTER_AUTH_KEYS", "test1:test_key_12345678,test2:another_secret_key")
    return mock_env_reader


@pytest.fixture
def mock_env_with_default_key(mock_env_reader):
    """
    Fixture that sets up a mock environment with a default key.
    """
    mock_env_reader.set_env("COMFY_CLUSTER_DEFAULT_KEY", "custom_default_key")
    return mock_env_reader


@pytest.fixture
def mock_env_with_server_config(mock_env_reader):
    """
    Fixture that sets up a mock environment with server configuration.
    """
    mock_env_reader.set_env("COMFY_CLUSTER_STUN_HOST", "127.0.0.1")
    mock_env_reader.set_env("COMFY_CLUSTER_STUN_PORT", "9000")
    mock_env_reader.set_env("COMFY_CLUSTER_STUN_LOG_LEVEL", "DEBUG")
    mock_env_reader.set_env("COMFY_CLUSTER_AUTH_KEYS", "test:test_key_12345678")
    return mock_env_reader