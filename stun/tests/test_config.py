#!/usr/bin/env python3
"""
Tests for the configuration module of the ComfyUI Cluster STUN Server.
"""

import pytest
from unittest.mock import patch

from src.config import EnvReader, ServerConfig, load_cluster_keys, get_config_from_env
from src.exceptions import ConfigurationError


class TestEnvReader:
    """Tests for the EnvReader class."""
    
    def test_get_existing_env_var(self):
        """Test getting an existing environment variable."""
        with patch('os.getenv') as mock_getenv:
            # Simulate os.getenv behavior more accurately
            mock_getenv.side_effect = lambda name, default=None: 'test_value' if name == 'TEST_VAR' else default
            reader = EnvReader()
            assert reader.get('TEST_VAR') == 'test_value'
    
    def test_get_non_existent_env_var_with_default(self):
        """Test getting a non-existent environment variable with a default value."""
        with patch('os.getenv') as mock_getenv:
            # Simulate os.getenv behavior more accurately
            mock_getenv.side_effect = lambda name, default=None: default if name == 'NON_EXISTENT_VAR' else None
            reader = EnvReader()
            assert reader.get('NON_EXISTENT_VAR', 'default_value') == 'default_value'


class TestServerConfig:
    """Tests for the ServerConfig class."""
    
    def test_valid_config(self, valid_server_config):
        """Test creating a valid server configuration."""
        # The fixture should create a valid config without raising exceptions
        assert valid_server_config.host == "127.0.0.1"
        assert valid_server_config.port == 8089
        assert valid_server_config.log_level == "INFO"
        assert valid_server_config.cluster_keys == {"test_cluster": "test_key_12345678"}
    
    def test_empty_host(self):
        """Test creating a configuration with an empty host."""
        with pytest.raises(ConfigurationError, match="Host cannot be empty"):
            ServerConfig(
                host="",
                port=8089,
                log_level="INFO",
                cluster_keys={"test": "test_key"}
            )
    
    def test_invalid_port_type(self):
        """Test creating a configuration with an invalid port type."""
        with pytest.raises(ConfigurationError, match="Invalid port number"):
            ServerConfig(
                host="127.0.0.1",
                port="not_a_number",
                log_level="INFO",
                cluster_keys={"test": "test_key"}
            )
    
    def test_invalid_port_range_low(self):
        """Test creating a configuration with a port number below the valid range."""
        with pytest.raises(ConfigurationError, match="Invalid port number"):
            ServerConfig(
                host="127.0.0.1",
                port=0,
                log_level="INFO",
                cluster_keys={"test": "test_key"}
            )
    
    def test_invalid_port_range_high(self):
        """Test creating a configuration with a port number above the valid range."""
        with pytest.raises(ConfigurationError, match="Invalid port number"):
            ServerConfig(
                host="127.0.0.1",
                port=65536,
                log_level="INFO",
                cluster_keys={"test": "test_key"}
            )
    
    def test_empty_log_level(self):
        """Test creating a configuration with an empty log level."""
        with pytest.raises(ConfigurationError, match="Log level cannot be empty"):
            ServerConfig(
                host="127.0.0.1",
                port=8089,
                log_level="",
                cluster_keys={"test": "test_key"}
            )
    
    def test_empty_cluster_keys(self, caplog):
        """Test creating a configuration with empty cluster keys (warning, not error)."""
        config = ServerConfig(
            host="127.0.0.1",
            port=8089,
            log_level="INFO",
            cluster_keys={}
        )
        assert "No cluster keys configured" in caplog.text
        assert config.cluster_keys == {}


class TestLoadClusterKeys:
    """Tests for the load_cluster_keys function."""
    
    def test_load_from_env(self, mock_env_with_cluster_keys):
        """Test loading cluster keys from environment variable."""
        keys = load_cluster_keys(mock_env_with_cluster_keys)
        assert keys == {
            "test1": "test_key_12345678",
            "test2": "another_secret_key"
        }
    
    def test_load_default_key(self, mock_env_reader):
        """Test loading default key when no cluster keys are configured."""
        # No COMFY_CLUSTER_AUTH_KEYS set, but COMFY_CLUSTER_DEFAULT_KEY is
        mock_env_reader.set_env("COMFY_CLUSTER_DEFAULT_KEY", "custom_default_key")
        keys = load_cluster_keys(mock_env_reader)
        assert keys == {"default": "custom_default_key"}
    
    def test_load_insecure_default_key(self, mock_env_reader, caplog):
        """Test loading insecure default key with warning."""
        # No environment variables set at all
        keys = load_cluster_keys(mock_env_reader)
        assert keys == {"default": "default_key_CHANGE_ME"}
        assert "Using insecure default cluster key" in caplog.text
    
    def test_invalid_key_pair_format(self, mock_env_reader):
        """Test invalid key pair format (missing colon)."""
        mock_env_reader.set_env("COMFY_CLUSTER_AUTH_KEYS", "test1-invalid-format")
        with pytest.raises(ConfigurationError, match="Invalid key pair format"):
            load_cluster_keys(mock_env_reader)
    
    def test_empty_cluster_id(self, mock_env_reader):
        """Test empty cluster ID in key pair."""
        mock_env_reader.set_env("COMFY_CLUSTER_AUTH_KEYS", ":test_key")
        with pytest.raises(ConfigurationError, match="Empty cluster ID found"):
            load_cluster_keys(mock_env_reader)
    
    def test_key_too_short(self, mock_env_reader):
        """Test cluster key that is too short."""
        mock_env_reader.set_env("COMFY_CLUSTER_AUTH_KEYS", "test:short")
        with pytest.raises(ConfigurationError, match="Cluster key for 'test' is too short"):
            load_cluster_keys(mock_env_reader)


class TestGetConfigFromEnv:
    """Tests for the get_config_from_env function."""
    
    def test_defaults(self, mock_env_with_cluster_keys):
        """Test getting configuration with only cluster keys set."""
        config = get_config_from_env(env_reader=mock_env_with_cluster_keys)
        assert config.host == "0.0.0.0"  # Default host
        assert config.port == 8089       # Default port
        assert config.log_level == "INFO"  # Default log level
        assert "test1" in config.cluster_keys
        assert "test2" in config.cluster_keys
    
    def test_full_config_from_env(self, mock_env_with_server_config):
        """Test getting full configuration from environment variables."""
        config = get_config_from_env(env_reader=mock_env_with_server_config)
        assert config.host == "127.0.0.1"
        assert config.port == 9000
        assert config.log_level == "DEBUG"
        assert "test" in config.cluster_keys
    
    def test_args_override_env(self, mock_env_with_server_config):
        """Test command line args overriding environment variables."""
        args = {
            "host": "192.168.1.1",
            "port": 8000,
            "log_level": "ERROR"
        }
        config = get_config_from_env(args=args, env_reader=mock_env_with_server_config)
        assert config.host == "192.168.1.1"  # From args
        assert config.port == 8000          # From args
        assert config.log_level == "ERROR"   # From args
        assert "test" in config.cluster_keys  # From env
    
    def test_invalid_port_in_env(self, mock_env_reader):
        """Test invalid port number in environment variable."""
        mock_env_reader.set_env("COMFY_CLUSTER_STUN_PORT", "not_a_number")
        mock_env_reader.set_env("COMFY_CLUSTER_AUTH_KEYS", "test:test_key_12345678")
        
        with pytest.raises(ConfigurationError, match="Invalid port number"):
            get_config_from_env(env_reader=mock_env_reader)
    
    def test_partial_args(self, mock_env_with_server_config):
        """Test partial command line args mixing with environment variables."""
        args = {
            "host": "192.168.1.1",
            # port and log_level not provided in args
        }
        config = get_config_from_env(args=args, env_reader=mock_env_with_server_config)
        assert config.host == "192.168.1.1"  # From args
        assert config.port == 9000          # From env
        assert config.log_level == "DEBUG"   # From env
    
    def test_empty_args(self, mock_env_with_server_config):
        """Test with empty args dictionary."""
        config = get_config_from_env(args={}, env_reader=mock_env_with_server_config)
        assert config.host == "127.0.0.1"  # From env
        assert config.port == 9000        # From env
        assert config.log_level == "DEBUG" # From env
    
    def test_none_args(self, mock_env_with_server_config):
        """Test with None args."""
        config = get_config_from_env(args=None, env_reader=mock_env_with_server_config)
        assert config.host == "127.0.0.1"  # From env
        assert config.port == 9000        # From env
        assert config.log_level == "DEBUG" # From env