#!/usr/bin/env python3
"""
Tests for the authentication module of the ComfyUI Cluster STUN Server.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.auth import (
    CLUSTER_KEY_HEADER, HeaderAPIKeyProvider, validate_cluster_id,
    validate_api_key, KeyBasedAuthProvider
)
from src.exceptions import ConfigurationError


@pytest.fixture
def mock_api_key_provider():
    """Fixture that provides a mock API key provider."""
    provider = AsyncMock()
    provider.return_value = "test_api_key"
    return provider


@pytest.fixture
def mock_cluster_keys():
    """Fixture that provides mock cluster keys for authentication."""
    return {
        "cluster1": "api_key_1",
        "cluster2": "api_key_2",
        "default": "default_key"
    }


@pytest.fixture
def auth_provider(mock_cluster_keys, mock_api_key_provider):
    """Fixture that provides an authentication provider."""
    return KeyBasedAuthProvider(
        cluster_keys=mock_cluster_keys,
        api_key_provider=mock_api_key_provider
    )


class TestHeaderAPIKeyProvider:
    """Tests for the HeaderAPIKeyProvider class."""
    
    @pytest.mark.asyncio
    async def test_get_api_key_from_header(self):
        """Test getting an API key from a header."""
        # Mock APIKeyHeader to return a specific value
        with patch('fastapi.security.APIKeyHeader.__call__', AsyncMock(return_value="test_key")):
            provider = HeaderAPIKeyProvider()
            api_key = await provider()
            assert api_key == "test_key"
    
    @pytest.mark.asyncio
    async def test_get_missing_api_key_from_header(self):
        """Test getting a missing API key from a header."""
        # Mock APIKeyHeader to return None (no key provided)
        with patch('fastapi.security.APIKeyHeader.__call__', AsyncMock(return_value=None)):
            provider = HeaderAPIKeyProvider()
            api_key = await provider()
            assert api_key is None
    
    def test_custom_header_name(self):
        """Test using a custom header name."""
        # Verify that the header name is passed to the APIKeyHeader constructor
        with patch('fastapi.security.APIKeyHeader.__init__', return_value=None) as mock_init:
            provider = HeaderAPIKeyProvider(header_name="Custom-Header")
            mock_init.assert_called_once_with(name="Custom-Header", auto_error=False)


class TestValidationFunctions:
    """Tests for cluster ID and API key validation functions."""
    
    def test_validate_cluster_id_valid(self):
        """Test validating a valid cluster ID."""
        assert validate_cluster_id("test_cluster") == "test_cluster"
    
    def test_validate_cluster_id_empty(self):
        """Test validating an empty cluster ID."""
        with pytest.raises(HTTPException) as excinfo:
            validate_cluster_id("")
        assert excinfo.value.status_code == 400
        assert "Cluster ID is required" in str(excinfo.value.detail)
    
    def test_validate_cluster_id_none(self):
        """Test validating a None cluster ID."""
        with pytest.raises(HTTPException) as excinfo:
            validate_cluster_id(None)
        assert excinfo.value.status_code == 400
        assert "Cluster ID is required" in str(excinfo.value.detail)
    
    def test_validate_api_key_valid(self):
        """Test validating a valid API key."""
        assert validate_api_key("test_key", "test_cluster") == "test_key"
    
    def test_validate_api_key_empty(self):
        """Test validating an empty API key."""
        with pytest.raises(HTTPException) as excinfo:
            validate_api_key("", "test_cluster")
        assert excinfo.value.status_code == 401
        assert "Cluster key is required" in str(excinfo.value.detail)
    
    def test_validate_api_key_none(self):
        """Test validating a None API key."""
        with pytest.raises(HTTPException) as excinfo:
            validate_api_key(None, "test_cluster")
        assert excinfo.value.status_code == 401
        assert "Cluster key is required" in str(excinfo.value.detail)


class TestKeyBasedAuthProvider:
    """Tests for the KeyBasedAuthProvider class."""
    
    def test_initialization(self, mock_cluster_keys):
        """Test initializing the authentication provider."""
        provider = KeyBasedAuthProvider(cluster_keys=mock_cluster_keys)
        assert provider.cluster_keys == mock_cluster_keys
    
    def test_initialization_empty_keys(self):
        """Test initializing the authentication provider with empty keys."""
        with pytest.raises(ConfigurationError, match="No cluster keys provided"):
            KeyBasedAuthProvider(cluster_keys={})
    
    def test_initialization_none_keys(self):
        """Test initializing the authentication provider with None keys."""
        with pytest.raises(ConfigurationError, match="No cluster keys provided"):
            KeyBasedAuthProvider(cluster_keys=None)
    
    def test_check_cluster_configured_valid(self, auth_provider):
        """Test checking if a cluster is configured."""
        assert auth_provider.check_cluster_configured("cluster1") is True
    
    def test_check_cluster_configured_invalid(self, auth_provider):
        """Test checking if a non-existent cluster is configured."""
        with pytest.raises(HTTPException) as excinfo:
            auth_provider.check_cluster_configured("nonexistent")
        assert excinfo.value.status_code == 404
        assert "not configured" in str(excinfo.value.detail)
    
    def test_verify_key_matches_valid(self, auth_provider):
        """Test verifying that a key matches."""
        assert auth_provider.verify_key_matches("cluster1", "api_key_1") is True
    
    def test_verify_key_matches_invalid(self, auth_provider):
        """Test verifying that a key does not match."""
        with pytest.raises(HTTPException) as excinfo:
            auth_provider.verify_key_matches("cluster1", "wrong_key")
        assert excinfo.value.status_code == 403
        assert "Invalid cluster key" in str(excinfo.value.detail)
    
    def test_authenticate_valid(self, auth_provider):
        """Test authenticating with valid credentials."""
        assert auth_provider.authenticate("cluster1", "api_key_1") is True
    
    def test_authenticate_invalid_cluster(self, auth_provider):
        """Test authenticating with an invalid cluster."""
        with pytest.raises(HTTPException) as excinfo:
            auth_provider.authenticate("nonexistent", "api_key_1")
        assert excinfo.value.status_code == 404
    
    def test_authenticate_invalid_key(self, auth_provider):
        """Test authenticating with an invalid key."""
        with pytest.raises(HTTPException) as excinfo:
            auth_provider.authenticate("cluster1", "wrong_key")
        assert excinfo.value.status_code == 403
    
    @pytest.mark.asyncio
    async def test_get_api_key(self, auth_provider, mock_api_key_provider):
        """Test getting an API key."""
        api_key = await auth_provider.get_api_key()
        assert api_key == "test_api_key"
        mock_api_key_provider.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_security_dependency(self, auth_provider):
        """Test the security dependency function."""
        # Mock the authenticate method to verify it's called correctly
        with patch.object(auth_provider, 'authenticate', return_value=True) as mock_authenticate:
            # Get the dependency function
            dependency = auth_provider.get_security_dependency()
            
            # Call with cluster_id specified
            assert await dependency(cluster_id="cluster1", api_key="api_key_1") is True
            mock_authenticate.assert_called_with("cluster1", "api_key_1")
            
            # Call with admin_cluster_id specified
            mock_authenticate.reset_mock()
            assert await dependency(admin_cluster_id="cluster2", api_key="api_key_2") is True
            mock_authenticate.assert_called_with("cluster2", "api_key_2")
    
    @pytest.mark.asyncio
    async def test_security_dependency_missing_cluster_id(self, auth_provider):
        """Test the security dependency function with missing cluster ID."""
        # Mock the validate_cluster_id function to verify it's called
        with patch('src.auth.validate_cluster_id', side_effect=HTTPException(status_code=400, detail="Test error")):
            # Get the dependency function
            dependency = auth_provider.get_security_dependency()
            
            # Call with no cluster_id or admin_cluster_id
            with pytest.raises(HTTPException) as excinfo:
                await dependency()
            assert excinfo.value.status_code == 400
    
    @pytest.mark.asyncio
    async def test_security_dependency_get_api_key(self, auth_provider):
        """Test the security dependency function getting an API key from the provider."""
        # Mock the authenticate method
        with patch.object(auth_provider, 'authenticate', return_value=True) as mock_authenticate:
            # Mock the get_api_key method
            with patch.object(auth_provider, 'get_api_key', AsyncMock(return_value="provider_key")) as mock_get_api_key:
                # Get the dependency function
                dependency = auth_provider.get_security_dependency()
                
                # Call with no api_key specified
                assert await dependency(cluster_id="cluster1") is True
                mock_get_api_key.assert_called_once()
                mock_authenticate.assert_called_with("cluster1", "provider_key")