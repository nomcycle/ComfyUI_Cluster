#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server

A simple STUN (Signaling and Tracking Usage for Networks) server for ComfyUI Cluster.
This server provides a central registration point for ComfyUI Cluster instances.

Usage:
    python server.py [--host HOST] [--port PORT]
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, List, Optional, Tuple, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("comfyui-cluster-stun")


# Data models
class InstanceRegistration(BaseModel):
    """Data model for instance registration."""
    address: str
    direct_listen_port: int
    role: str
    instance_id: int
    cluster_id: str
    cluster_key: str


class InstanceInfo(BaseModel):
    """Data model for instance information."""
    instance_id: int
    address: str
    direct_listen_port: int
    role: str
    last_seen: float


class ClusterInfo(BaseModel):
    """Data model for cluster information."""
    instances: List[InstanceInfo]
    last_change_time: float


# Server state
class ClusterState:
    """State for a single cluster."""
    def __init__(self):
        # instance_id -> (address, port, role, last_ping)
        self.instances: Dict[int, Tuple[str, int, str, float]] = {}
        self.last_change_time = float = time.time()

    def register_instance(self, instance_id: int, address: str, port: int, role: str) -> None:
        """Register or update an instance."""
        self.instances[instance_id] = (address, port, role, time.time())
        self.last_change_time = time.time()

    def remove_instance(self, instance_id: int) -> None:
        """Remove an instance from the cluster."""
        if instance_id in self.instances:
            del self.instances[instance_id]
            self.last_change_time = time.time()
    
    def update_heartbeat(self, instance_id: int) -> bool:
        """Update the heartbeat time for an instance. Returns True if successful."""
        if instance_id not in self.instances:
            return False
        
        address, port, role, _ = self.instances[instance_id]
        self.instances[instance_id] = (address, port, role, time.time())
        return True

    def get_instances(self) -> List[Dict[str, Any]]:
        """Get all registered instances."""
        return [
            {
                "instance_id": instance_id,
                "address": address,
                "direct_listen_port": port,
                "role": role,
                "last_seen": last_seen
            }
            for instance_id, (address, port, role, last_seen) in self.instances.items()
        ]
    
    def remove_stale_instances(self, max_age_seconds: int = 60) -> List[int]:
        """Remove instances that haven't been seen recently. Returns list of removed IDs."""
        current_time = time.time()
        stale_instances = [
            instance_id for instance_id, (_, _, _, last_seen) in self.instances.items()
            if (current_time - last_seen) > max_age_seconds
        ]
        
        for instance_id in stale_instances:
            self.remove_instance(instance_id)
            
        return stale_instances


# Global state
clusters: Dict[str, ClusterState] = {}

# Predefined cluster keys - should be loaded from a secure source
# In a real-world deployment, this would come from environment variables or a secure config file
CLUSTER_KEYS: Dict[str, str] = {}

# Authentication
CLUSTER_KEY_HEADER = "X-Cluster-Key"
cluster_key_header = APIKeyHeader(name=CLUSTER_KEY_HEADER, auto_error=False)

async def authenticate_cluster(
    cluster_id: str, 
    api_key: str = Security(cluster_key_header)
) -> bool:
    """
    Authenticate a request using the cluster key.
    
    Args:
        cluster_id: The ID of the cluster
        api_key: The API key provided in the request header
        
    Returns:
        True if authentication is successful
        
    Raises:
        HTTPException: If authentication fails
    """
    # Validate input
    if not cluster_id:
        logger.warning("Missing cluster ID in authentication request")
        raise HTTPException(
            status_code=400,
            detail="Cluster ID is required"
        )
    
    if not api_key:
        logger.warning(f"Missing cluster key in authentication request for cluster: {cluster_id}")
        raise HTTPException(
            status_code=401,
            detail="Cluster key is required",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    # Check if this cluster ID is configured
    if cluster_id not in CLUSTER_KEYS:
        logger.warning(f"Attempt to access unconfigured cluster ID: {cluster_id}")
        raise HTTPException(
            status_code=404,
            detail=f"Cluster ID '{cluster_id}' is not configured on this server"
        )
    
    # Check if the key matches the predefined key for this cluster
    if api_key != CLUSTER_KEYS[cluster_id]:
        logger.warning(f"Invalid cluster key for cluster: {cluster_id}")
        raise HTTPException(
            status_code=403,
            detail="Invalid cluster key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    return True


# FastAPI app
app = FastAPI(
    title="ComfyUI Cluster STUN Server",
    description="STUN server for ComfyUI Cluster instance registration",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Background tasks
async def cleanup_stale_instances(max_age_seconds: int = 60):
    """Remove instances that haven't been seen recently."""
    for cluster_id, cluster in clusters.items():
        removed_instances = cluster.remove_stale_instances(max_age_seconds)
        if removed_instances:
            logger.info(f"Removed stale instances from cluster {cluster_id}: {removed_instances}")


# API routes
@app.post("/register-instance")
async def register_instance(
    registration: InstanceRegistration, 
    background_tasks: BackgroundTasks,
    authenticated: bool = Depends(lambda: authenticate_cluster(registration.cluster_id, registration.cluster_key))
):
    """Register an instance with the STUN server."""
    # The authenticate_cluster dependency will validate the cluster key
    # If we get here, authentication has already succeeded
    
    # Create cluster state if it doesn't exist
    if registration.cluster_id not in clusters:
        clusters[registration.cluster_id] = ClusterState()
    
    # Register instance
    cluster = clusters[registration.cluster_id]
    cluster.register_instance(
        registration.instance_id,
        registration.address,
        registration.direct_listen_port,
        registration.role
    )
    
    # Log registration
    logger.info(
        f"Registered instance {registration.instance_id} in cluster "
        f"{registration.cluster_id} at {registration.address}:{registration.direct_listen_port}"
    )
    
    # Trigger cleanup of stale instances
    background_tasks.add_task(cleanup_stale_instances)
    
    # Return success
    return {
        "status": "registered",
        "instance_count": len(cluster.instances),
        "instance_id": registration.instance_id
    }


@app.get("/instances/{cluster_id}")
async def get_instances(
    cluster_id: str,
    authenticated: bool = Depends(lambda: authenticate_cluster(cluster_id, cluster_key_header()))
):
    """Get all instances for a cluster."""
    if cluster_id not in clusters:
        raise HTTPException(status_code=404, detail=f"Cluster '{cluster_id}' not found")
    
    cluster = clusters[cluster_id]
    return {
        "instances": cluster.get_instances(),
        "last_change_time": cluster.last_change_time
    }


@app.post("/heartbeat/{cluster_id}/{instance_id}")
async def heartbeat(
    cluster_id: str, 
    instance_id: int,
    authenticated: bool = Depends(lambda: authenticate_cluster(cluster_id, cluster_key_header()))
):
    """Update heartbeat for an instance."""
    if cluster_id not in clusters:
        raise HTTPException(status_code=404, detail=f"Cluster '{cluster_id}' not found")
    
    cluster = clusters[cluster_id]
    if not cluster.update_heartbeat(instance_id):
        raise HTTPException(status_code=404, detail=f"Instance {instance_id} not found")
        
    return {"status": "ok"}


@app.get("/clusters/{admin_cluster_id}")
async def get_clusters(
    admin_cluster_id: str,
    authenticated: bool = Depends(lambda: authenticate_cluster(admin_cluster_id, cluster_key_header()))
):
    """
    Get all registered clusters.
    
    The admin_cluster_id parameter is used for authentication and should be a valid cluster ID.
    This adds consistency with other endpoints while maintaining the same functionality.
    """
    # Authentication is handled by the authenticate_cluster dependency
    # If we're here, it means authentication succeeded
    
    return {
        "clusters": list(clusters.keys()),
        "count": len(clusters)
    }


@app.delete("/instance/{cluster_id}/{instance_id}")
async def remove_instance(
    cluster_id: str, 
    instance_id: int,
    authenticated: bool = Depends(lambda: authenticate_cluster(cluster_id, cluster_key_header()))
):
    """Manually remove an instance."""
    if cluster_id not in clusters:
        raise HTTPException(status_code=404, detail=f"Cluster '{cluster_id}' not found")
    
    cluster = clusters[cluster_id]
    if instance_id not in cluster.instances:
        raise HTTPException(status_code=404, detail=f"Instance {instance_id} not found")
    
    cluster.remove_instance(instance_id)
    logger.info(f"Manually removed instance {instance_id} from cluster {cluster_id}")
    
    return {"status": "removed"}


@app.get("/health")
async def health_check():
    """Health check endpoint (no authentication required)."""
    # This endpoint is left open for health checks and monitoring
    return {
        "status": "healthy", 
        "timestamp": time.time(),
        "clusters_configured": len(CLUSTER_KEYS),
        "clusters_active": len(clusters)
    }


# Shutdown signal handling
def handle_shutdown(signum, frame):
    """Handle shutdown signals."""
    logger.info("Received shutdown signal, exiting...")
    sys.exit(0)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def load_cluster_keys():
    """Load predefined cluster keys from environment variables."""
    # Format: COMFY_CLUSTER_AUTH_KEYS="cluster1:key1,cluster2:key2"
    auth_keys_env = os.getenv("COMFY_CLUSTER_AUTH_KEYS", "")
    
    if not auth_keys_env:
        logger.warning("No predefined cluster keys found. Set COMFY_CLUSTER_AUTH_KEYS for better security.")
        # For backward compatibility, we'll allow the default cluster ID with a default key
        CLUSTER_KEYS["default"] = os.getenv("COMFY_CLUSTER_DEFAULT_KEY", "default_key_CHANGE_ME")
        logger.warning(f"Using default cluster key for 'default' cluster ID. CHANGE THIS IN PRODUCTION!")
        return
    
    # Parse the environment variable
    try:
        key_pairs = auth_keys_env.split(",")
        for pair in key_pairs:
            if ":" not in pair:
                logger.warning(f"Invalid key pair format: {pair}, skipping")
                continue
                
            cluster_id, key = pair.split(":", 1)
            cluster_id = cluster_id.strip()
            key = key.strip()
            
            if len(key) < 8:
                logger.warning(f"Cluster key for '{cluster_id}' is too short (<8 chars), skipping")
                continue
                
            CLUSTER_KEYS[cluster_id] = key
            # Log only part of the key for security
            masked_key = key[:3] + "*" * (len(key) - 6) + key[-3:]
            logger.info(f"Loaded auth key for cluster: {cluster_id} ({masked_key})")
    
    except Exception as e:
        logger.error(f"Error parsing COMFY_CLUSTER_AUTH_KEYS: {str(e)}")
        raise

def main():
    """Main entrypoint for running the server directly."""
    parser = argparse.ArgumentParser(description="ComfyUI Cluster STUN Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to listen on")
    parser.add_argument("--port", type=int, default=8089, help="Port to listen on")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure log level
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")
    logger.setLevel(numeric_level)
    
    # Load predefined cluster keys
    load_cluster_keys()
    
    # Print banner
    logger.info("Starting ComfyUI Cluster STUN Server")
    logger.info(f"Listening on {args.host}:{args.port}")
    logger.info(f"Configured authentication for {len(CLUSTER_KEYS)} clusters")
    
    # Start server
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()