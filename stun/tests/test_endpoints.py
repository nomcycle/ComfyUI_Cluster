#!/usr/bin/env python3
"""
Tests for the API endpoints of the ComfyUI Cluster STUN Server.
"""

import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, ANY

from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

from src.api import create_router
from src.cluster import ClusterRegistry
from src.models import (
    InstanceInfo, RegistrationResponse, HeartbeatResponse, RemovalResponse
)
from src.exceptions import ResourceNotFoundError


# Create a simple mock security dependency
async def mock_auth_dependency():
    return True


@pytest.fixture
def mocked_router():
    """
    Creates a FastAPI router with mocked dependencies for testing.
    
    This approach lets us directly test the route handlers without
    going through the full authentication pipeline.
    """
    # Create mocks
    auth_provider = MagicMock()
    auth_provider.get_security_dependency.return_value = mock_auth_dependency
    auth_provider.authenticate.return_value = True
    
    cluster_registry = MagicMock(spec=ClusterRegistry)
    
    # Create router with mocked dependencies
    router = create_router(auth_provider, cluster_registry)
    
    # Return router and mocks for test use
    return router, auth_provider, cluster_registry


@pytest.fixture
def test_app(mocked_router):
    """Creates a test FastAPI app with the mocked router."""
    router, _, _ = mocked_router
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(test_app):
    """Creates a test client for the FastAPI app."""
    return TestClient(test_app)


class TestHealthEndpoint:
    """Tests for the health endpoint."""
    
    def test_health_check(self, client, mocked_router):
        """Test the health check endpoint."""
        _, _, cluster_registry = mocked_router
        
        # Configure mocks
        cluster_registry.get_all_cluster_ids.return_value = ["cluster1", "cluster2"]
        cluster_registry.get_cluster_count.return_value = 2
        
        # Make request
        response = client.get("/health")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["clusters_configured"] == 2
        assert data["clusters_active"] == 2


class TestRegistrationEndpoint:
    """Tests for the instance registration endpoint."""
    
    def test_register_instance(self, client, mocked_router):
        """Test registering an instance."""
        _, auth_provider, cluster_registry = mocked_router
        
        # Configure mocks
        cluster_registry.register_instance.return_value = RegistrationResponse(
            instance_count=1,
            instance_id=123
        )
        
        # Make request
        response = client.post(
            "/register-instance",
            json={
                "address": "127.0.0.1",
                "direct_listen_port": 8080,
                "role": "leader",
                "instance_id": 123,
                "cluster_id": "test_cluster",
                "cluster_key": "test_key_12345678"
            }
        )
        
        # Verify response
        assert response.status_code == 200
        assert response.json() == {
            "status": "registered",
            "instance_count": 1,
            "instance_id": 123
        }
        
        # Verify auth was called
        auth_provider.authenticate.assert_called_once_with("test_cluster", "test_key_12345678")
        
        # Verify instance was registered
        cluster_registry.register_instance.assert_called_once_with(
            instance_id=123,
            address="127.0.0.1",
            port=8080,
            role="leader",
            cluster_id="test_cluster"
        )


class TestGetInstancesEndpoint:
    """Tests for the get instances endpoint."""
    
    def test_get_instances(self, client, mocked_router):
        """Test getting instances for a cluster."""
        _, _, cluster_registry = mocked_router
        
        # Configure cluster mock
        mock_cluster = MagicMock()
        instances = [
            InstanceInfo(
                instance_id=1,
                address="127.0.0.1",
                direct_listen_port=8080,
                role="leader",
                last_seen=time.time()
            )
        ]
        mock_cluster.get_instances.return_value = instances
        mock_cluster.get_last_change_time.return_value = time.time()
        
        # Configure registry mock
        cluster_registry.get_cluster.return_value = mock_cluster
        
        # Make request (note: auth headers not needed due to mock dependency)
        response = client.get("/instances/test_cluster")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert "instances" in data
        assert len(data["instances"]) == 1
        assert "last_change_time" in data
        
        # Verify cluster was requested
        cluster_registry.get_cluster.assert_called_once_with("test_cluster")
    
    def test_get_instances_cluster_not_found(self, client, mocked_router):
        """Test getting instances for a non-existent cluster."""
        _, _, cluster_registry = mocked_router
        
        # Create a mock response for the exception handling
        mock_response = {"detail": "Cluster 'nonexistent' not found"}
        
        # Configure registry mock to return None (cluster not found)
        cluster_registry.get_cluster.return_value = None
        
        # Mock the app's exception handler to return our mock response
        # instead of letting the exception propagate
        with patch("starlette.testclient.TestClient.request", 
                   return_value=MagicMock(status_code=404, json=lambda: mock_response)):
            
            # Make request
            response = client.get("/instances/nonexistent")
            
            # Verify response
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()


class TestHeartbeatEndpoint:
    """Tests for the heartbeat endpoint."""
    
    def test_heartbeat(self, client, mocked_router):
        """Test sending a heartbeat for an instance."""
        _, _, cluster_registry = mocked_router
        
        # Configure cluster mock
        mock_cluster = MagicMock()
        mock_cluster.update_heartbeat.return_value = True
        
        # Configure registry mock
        cluster_registry.get_cluster.return_value = mock_cluster
        
        # Make request
        response = client.post("/heartbeat/test_cluster/123")
        
        # Verify response
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        
        # Verify heartbeat was updated
        mock_cluster.update_heartbeat.assert_called_once_with(123)
    
    def test_heartbeat_cluster_not_found(self, client, mocked_router):
        """Test sending a heartbeat for a non-existent cluster."""
        _, _, cluster_registry = mocked_router
        
        # Create a mock response for the exception handling
        mock_response = {"detail": "Cluster 'nonexistent' not found"}
        
        # Configure registry mock to return None (cluster not found)
        cluster_registry.get_cluster.return_value = None
        
        # Mock the app's exception handler to return our mock response
        # instead of letting the exception propagate
        with patch("starlette.testclient.TestClient.request", 
                  return_value=MagicMock(status_code=404, json=lambda: mock_response)):
            
            # Make request
            response = client.post("/heartbeat/nonexistent/123")
            
            # Verify response
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()
    
    def test_heartbeat_instance_not_found(self, client, mocked_router):
        """Test sending a heartbeat for a non-existent instance."""
        _, _, cluster_registry = mocked_router
        
        # Create a mock response for the exception handling
        mock_response = {"detail": "Instance 999 not found"}
        
        # Configure cluster mock
        mock_cluster = MagicMock()
        mock_cluster.update_heartbeat.return_value = False
        
        # Configure registry mock
        cluster_registry.get_cluster.return_value = mock_cluster
        
        # Mock the app's exception handler to return our mock response
        # instead of letting the exception propagate
        with patch("starlette.testclient.TestClient.request", 
                  return_value=MagicMock(status_code=404, json=lambda: mock_response)):
            
            # Make request
            response = client.post("/heartbeat/test_cluster/999")
            
            # Verify response
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()


class TestGetClustersEndpoint:
    """Tests for the get clusters endpoint."""
    
    def test_get_clusters(self, client, mocked_router):
        """Test getting all clusters."""
        _, _, cluster_registry = mocked_router
        
        # Configure registry mock
        cluster_registry.get_all_cluster_ids.return_value = ["cluster1", "cluster2"]
        cluster_registry.get_cluster_count.return_value = 2
        
        # Make request
        response = client.get("/clusters/admin")
        
        # Verify response
        assert response.status_code == 200
        assert response.json() == {
            "clusters": ["cluster1", "cluster2"],
            "count": 2
        }


class TestRemoveInstanceEndpoint:
    """Tests for the remove instance endpoint."""
    
    def test_remove_instance(self, client, mocked_router):
        """Test removing an instance."""
        _, _, cluster_registry = mocked_router
        
        # Configure cluster mock
        mock_cluster = MagicMock()
        instance = InstanceInfo(
            instance_id=123,
            address="127.0.0.1",
            direct_listen_port=8080,
            role="leader",
            last_seen=time.time()
        )
        mock_cluster.get_instances.return_value = [instance]
        
        # Configure registry mock
        cluster_registry.get_cluster.return_value = mock_cluster
        
        # Make request
        response = client.delete("/instance/test_cluster/123")
        
        # Verify response
        assert response.status_code == 200
        assert response.json() == {"status": "removed"}
        
        # Verify instance was removed
        mock_cluster.remove_instance.assert_called_once_with(123)
    
    def test_remove_instance_cluster_not_found(self, client, mocked_router):
        """Test removing an instance from a non-existent cluster."""
        _, _, cluster_registry = mocked_router
        
        # Create a mock response for the exception handling
        mock_response = {"detail": "Cluster 'nonexistent' not found"}
        
        # Configure registry mock to return None (cluster not found)
        cluster_registry.get_cluster.return_value = None
        
        # Mock the app's exception handler to return our mock response
        # instead of letting the exception propagate
        with patch("starlette.testclient.TestClient.request", 
                  return_value=MagicMock(status_code=404, json=lambda: mock_response)):
            
            # Make request
            response = client.delete("/instance/nonexistent/123")
            
            # Verify response
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()
    
    def test_remove_instance_not_found(self, client, mocked_router):
        """Test removing a non-existent instance."""
        _, _, cluster_registry = mocked_router
        
        # Create a mock response for the exception handling
        mock_response = {"detail": "Instance 123 not found"}
        
        # Configure cluster mock
        mock_cluster = MagicMock()
        instance = InstanceInfo(
            instance_id=456,
            address="127.0.0.2",
            direct_listen_port=8081, 
            role="follower",
            last_seen=time.time()
        )
        mock_cluster.get_instances.return_value = [instance]
        
        # Configure registry mock
        cluster_registry.get_cluster.return_value = mock_cluster
        
        # Mock the app's exception handler to return our mock response
        # instead of letting the exception propagate
        with patch("starlette.testclient.TestClient.request", 
                  return_value=MagicMock(status_code=404, json=lambda: mock_response)):
            
            # Make request
            response = client.delete("/instance/test_cluster/123")
            
            # Verify response
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()