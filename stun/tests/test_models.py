#!/usr/bin/env python3
"""
Tests for the models module of the ComfyUI Cluster STUN Server.
"""

import time
import pytest
from unittest.mock import patch

from pydantic import ValidationError as PydanticValidationError

from src.models import (
    validate_address, validate_port, validate_role, validate_instance_id, 
    validate_cluster_id, validate_cluster_key, InstanceRegistration, 
    InstanceInfo, ClusterInfo, HealthStatus, RegistrationResponse, 
    HeartbeatResponse, RemovalResponse
)

class TestValidationFunctions:
    """Tests for the validation helper functions."""
    
    def test_validate_address_valid(self):
        """Test validating a valid address."""
        assert validate_address("127.0.0.1") == "127.0.0.1"
        assert validate_address("example.com") == "example.com"
    
    def test_validate_address_empty(self):
        """Test validating an empty address."""
        with pytest.raises(ValueError, match="Address cannot be empty"):
            validate_address("")
    
    def test_validate_port_valid(self):
        """Test validating a valid port."""
        assert validate_port(1) == 1
        assert validate_port(8080) == 8080
        assert validate_port(65535) == 65535
    
    def test_validate_port_invalid_low(self):
        """Test validating a port that is too low."""
        with pytest.raises(ValueError, match="Invalid port number"):
            validate_port(0)
    
    def test_validate_port_invalid_high(self):
        """Test validating a port that is too high."""
        with pytest.raises(ValueError, match="Invalid port number"):
            validate_port(65536)
    
    def test_validate_role_valid(self):
        """Test validating a valid role."""
        assert validate_role("leader") == "leader"
        assert validate_role("follower") == "follower"
        # Test with custom valid roles
        assert validate_role("custom", {"custom", "other"}) == "custom"
    
    def test_validate_role_invalid(self):
        """Test validating an invalid role."""
        with pytest.raises(ValueError, match="Invalid role"):
            validate_role("invalid")
    
    def test_validate_instance_id_valid(self):
        """Test validating a valid instance ID."""
        assert validate_instance_id(0) == 0
        assert validate_instance_id(1) == 1
        assert validate_instance_id(9999) == 9999
    
    def test_validate_instance_id_invalid(self):
        """Test validating an invalid instance ID."""
        with pytest.raises(ValueError, match="Invalid instance ID"):
            validate_instance_id(-1)
    
    def test_validate_cluster_id_valid(self):
        """Test validating a valid cluster ID."""
        assert validate_cluster_id("test") == "test"
        assert validate_cluster_id("test-cluster") == "test-cluster"
    
    def test_validate_cluster_id_empty(self):
        """Test validating an empty cluster ID."""
        with pytest.raises(ValueError, match="Cluster ID cannot be empty"):
            validate_cluster_id("")
    
    def test_validate_cluster_key_valid(self):
        """Test validating a valid cluster key."""
        assert validate_cluster_key("12345678") == "12345678"
        assert validate_cluster_key("secret_key_12345") == "secret_key_12345"
    
    def test_validate_cluster_key_empty(self):
        """Test validating an empty cluster key."""
        with pytest.raises(ValueError, match="Cluster key cannot be empty"):
            validate_cluster_key("")
    
    def test_validate_cluster_key_too_short(self):
        """Test validating a cluster key that is too short."""
        with pytest.raises(ValueError, match="Cluster key is too short"):
            validate_cluster_key("short")
    
    def test_validate_cluster_key_custom_min_length(self):
        """Test validating a cluster key with a custom minimum length."""
        # Should pass with default min_length (8)
        assert validate_cluster_key("12345678") == "12345678"
        
        # Should fail with custom min_length (10)
        with pytest.raises(ValueError, match="Cluster key is too short"):
            validate_cluster_key("12345678", min_length=10)
        
        # Should pass with custom min_length (6)
        assert validate_cluster_key("123456", min_length=6) == "123456"


class TestInstanceRegistration:
    """Tests for the InstanceRegistration model."""
    
    def test_valid_instance_registration(self):
        """Test creating a valid instance registration."""
        registration = InstanceRegistration(
            address="127.0.0.1",
            direct_listen_port=8080,
            role="leader",
            instance_id=1,
            cluster_id="test",
            cluster_key="test_key_12345678"
        )
        assert registration.address == "127.0.0.1"
        assert registration.direct_listen_port == 8080
        assert registration.role == "leader"
        assert registration.instance_id == 1
        assert registration.cluster_id == "test"
        assert registration.cluster_key == "test_key_12345678"
    
    def test_invalid_address(self):
        """Test creating an instance registration with an invalid address."""
        with pytest.raises(PydanticValidationError):
            InstanceRegistration(
                address="",
                direct_listen_port=8080,
                role="leader",
                instance_id=1,
                cluster_id="test",
                cluster_key="test_key_12345678"
            )
    
    def test_invalid_port(self):
        """Test creating an instance registration with an invalid port."""
        with pytest.raises(PydanticValidationError):
            InstanceRegistration(
                address="127.0.0.1",
                direct_listen_port=0,
                role="leader",
                instance_id=1,
                cluster_id="test",
                cluster_key="test_key_12345678"
            )
    
    def test_invalid_role(self):
        """Test creating an instance registration with an invalid role."""
        with pytest.raises(PydanticValidationError):
            InstanceRegistration(
                address="127.0.0.1",
                direct_listen_port=8080,
                role="invalid",
                instance_id=1,
                cluster_id="test",
                cluster_key="test_key_12345678"
            )
    
    def test_invalid_instance_id(self):
        """Test creating an instance registration with an invalid instance ID."""
        with pytest.raises(PydanticValidationError):
            InstanceRegistration(
                address="127.0.0.1",
                direct_listen_port=8080,
                role="leader",
                instance_id=-1,
                cluster_id="test",
                cluster_key="test_key_12345678"
            )
    
    def test_invalid_cluster_id(self):
        """Test creating an instance registration with an invalid cluster ID."""
        with pytest.raises(PydanticValidationError):
            InstanceRegistration(
                address="127.0.0.1",
                direct_listen_port=8080,
                role="leader",
                instance_id=1,
                cluster_id="",
                cluster_key="test_key_12345678"
            )
    
    def test_invalid_cluster_key(self):
        """Test creating an instance registration with an invalid cluster key."""
        with pytest.raises(PydanticValidationError):
            InstanceRegistration(
                address="127.0.0.1",
                direct_listen_port=8080,
                role="leader",
                instance_id=1,
                cluster_id="test",
                cluster_key="short"
            )


class TestInstanceInfo:
    """Tests for the InstanceInfo model."""
    
    def test_valid_instance_info(self):
        """Test creating a valid instance info."""
        instance = InstanceInfo(
            instance_id=1,
            address="127.0.0.1",
            direct_listen_port=8080,
            role="leader",
            last_seen=time.time()
        )
        assert instance.instance_id == 1
        assert instance.address == "127.0.0.1"
        assert instance.direct_listen_port == 8080
        assert instance.role == "leader"
        assert isinstance(instance.last_seen, float)
    
    def test_default_last_seen(self):
        """Test that last_seen defaults to current time if not provided."""
        # Instead of patching time.time, we'll just verify the type
        instance = InstanceInfo(
            instance_id=1,
            address="127.0.0.1",
            direct_listen_port=8080,
            role="leader"
        )
        assert isinstance(instance.last_seen, float)
        # Ensure the timestamp is recent (within the last second)
        assert time.time() - instance.last_seen < 1.0
    
    def test_invalid_instance_id(self):
        """Test creating an instance info with an invalid instance ID."""
        with pytest.raises(PydanticValidationError):
            InstanceInfo(
                instance_id=-1,
                address="127.0.0.1",
                direct_listen_port=8080,
                role="leader"
            )
    
    def test_invalid_address(self):
        """Test creating an instance info with an invalid address."""
        with pytest.raises(PydanticValidationError):
            InstanceInfo(
                instance_id=1,
                address="",
                direct_listen_port=8080,
                role="leader"
            )
    
    def test_invalid_port(self):
        """Test creating an instance info with an invalid port."""
        with pytest.raises(PydanticValidationError):
            InstanceInfo(
                instance_id=1,
                address="127.0.0.1",
                direct_listen_port=0,
                role="leader"
            )
    
    def test_invalid_role(self):
        """Test creating an instance info with an invalid role."""
        with pytest.raises(PydanticValidationError):
            InstanceInfo(
                instance_id=1,
                address="127.0.0.1",
                direct_listen_port=8080,
                role="invalid"
            )


class TestClusterInfo:
    """Tests for the ClusterInfo model."""
    
    def test_valid_cluster_info(self):
        """Test creating a valid cluster info."""
        instance = InstanceInfo(
            instance_id=1,
            address="127.0.0.1",
            direct_listen_port=8080,
            role="leader"
        )
        cluster = ClusterInfo(
            instances=[instance],
            last_change_time=time.time()
        )
        assert len(cluster.instances) == 1
        assert isinstance(cluster.last_change_time, float)
    
    def test_empty_instances(self):
        """Test creating a cluster info with no instances."""
        cluster = ClusterInfo(
            instances=[],
            last_change_time=time.time()
        )
        assert len(cluster.instances) == 0
    
    def test_default_last_change_time(self):
        """Test that last_change_time defaults to current time if not provided."""
        instance = InstanceInfo(
            instance_id=1,
            address="127.0.0.1",
            direct_listen_port=8080,
            role="leader"
        )
        cluster = ClusterInfo(instances=[instance])
        assert isinstance(cluster.last_change_time, float)
        # Ensure the timestamp is recent (within the last second)
        assert time.time() - cluster.last_change_time < 1.0


class TestResponseModels:
    """Tests for the response models."""
    
    def test_health_status(self):
        """Test creating a health status response."""
        status = HealthStatus(
            clusters_configured=2,
            clusters_active=1
        )
        assert status.status == "healthy"
        assert isinstance(status.timestamp, float)
        # Ensure the timestamp is recent (within the last second)
        assert time.time() - status.timestamp < 1.0
        assert status.clusters_configured == 2
        assert status.clusters_active == 1
    
    def test_registration_response(self):
        """Test creating a registration response."""
        response = RegistrationResponse(
            instance_count=5,
            instance_id=1
        )
        assert response.status == "registered"
        assert response.instance_count == 5
        assert response.instance_id == 1
    
    def test_registration_response_invalid_instance_id(self):
        """Test creating a registration response with an invalid instance ID."""
        with pytest.raises(PydanticValidationError):
            RegistrationResponse(
                instance_count=5,
                instance_id=-1
            )
    
    def test_registration_response_invalid_instance_count(self):
        """Test creating a registration response with an invalid instance count."""
        with pytest.raises(PydanticValidationError):
            RegistrationResponse(
                instance_count=-1,
                instance_id=1
            )
    
    def test_heartbeat_response(self):
        """Test creating a heartbeat response."""
        response = HeartbeatResponse()
        assert response.status == "ok"
    
    def test_removal_response(self):
        """Test creating a removal response."""
        response = RemovalResponse()
        assert response.status == "removed"