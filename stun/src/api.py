#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - API Module

This module defines the FastAPI routes for the STUN server.
Implements simplified patterns while maintaining fail-fast principles.
"""

import logging
import time
from typing import Dict, List, Callable, Awaitable, Any

from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler

from .auth import KeyBasedAuthProvider
from .cluster import ClusterRegistry
from .exceptions import ConfigurationError, ValidationError, ResourceNotFoundError, AuthenticationError
from .models import (
    ClusterInfo, 
    HealthStatus, 
    HeartbeatResponse, 
    InstanceRegistration, 
    RegistrationResponse, 
    RemovalResponse
)

logger = logging.getLogger("comfyui-cluster-stun.api")

def setup_exception_handlers(app: FastAPI) -> None:
    """
    Set up exception handlers for the application.
    
    Args:
        app: FastAPI application
    """
    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError):
        logger.warning(f"Validation error: {str(exc)}")
        return JSONResponse(
            status_code=400,
            content={"detail": str(exc)},
        )
    
    @app.exception_handler(ResourceNotFoundError)
    async def resource_not_found_exception_handler(request: Request, exc: ResourceNotFoundError):
        logger.warning(f"Resource not found: {str(exc)}")
        return JSONResponse(
            status_code=404,
            content={"detail": str(exc)},
        )
    
    @app.exception_handler(AuthenticationError)
    async def auth_exception_handler(request: Request, exc: AuthenticationError):
        logger.warning(f"Authentication error: {str(exc)}")
        return JSONResponse(
            status_code=401,
            content={"detail": str(exc)},
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        if isinstance(exc, HTTPException):
            return await http_exception_handler(request, exc)
        
        logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

def setup_middleware(app: FastAPI) -> None:
    """
    Set up middleware for the application.
    
    Args:
        app: FastAPI application
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

async def cleanup_stale_instances(
    cluster_registry: ClusterRegistry,
    max_age_seconds: int = 60
) -> None:
    """
    Remove instances that haven't been seen recently.
    
    Args:
        cluster_registry: Registry for cluster state management
        max_age_seconds: Maximum age in seconds before an instance is considered stale
    """
    try:
        removed = cluster_registry.cleanup_stale_instances(max_age_seconds)
        if removed:
            for cluster_id, instances in removed.items():
                logger.info(f"Removed stale instances from cluster {cluster_id}: {instances}")
    except Exception as e:
        logger.error(f"Error cleaning up stale instances: {str(e)}", exc_info=True)

def create_router(
    auth_provider: KeyBasedAuthProvider,
    cluster_registry: ClusterRegistry,
) -> APIRouter:
    """
    Create the API router with all routes.
    
    Args:
        auth_provider: Provider for authentication
        cluster_registry: Registry for cluster state management
        
    Returns:
        Configured APIRouter
    """
    router = APIRouter()
    
    # Get authentication dependency
    authenticate_cluster = auth_provider.get_security_dependency()
    
    @router.post("/register-instance")
    async def register_instance(
        registration: InstanceRegistration, 
        background_tasks: BackgroundTasks
    ) -> RegistrationResponse:
        """Register an instance with the STUN server."""
        # Handle authentication separately for this endpoint since we have credentials in the body
        auth_provider.authenticate(registration.cluster_id, registration.cluster_key)
        
        # Register instance directly
        response = cluster_registry.register_instance(
            instance_id=registration.instance_id,
            address=registration.address,
            port=registration.direct_listen_port,
            role=registration.role,
            cluster_id=registration.cluster_id
        )
        
        # Trigger cleanup of stale instances
        background_tasks.add_task(
            cleanup_stale_instances,
            cluster_registry
        )
        
        return response
    
    @router.get("/instances/{cluster_id}")
    async def get_instances(
        cluster_id: str,
        authenticated: bool = Depends(authenticate_cluster)
    ) -> ClusterInfo:
        """Get all instances for a cluster."""
        cluster = cluster_registry.get_cluster(cluster_id)
        if not cluster:
            raise ResourceNotFoundError(f"Cluster '{cluster_id}' not found")
        
        return ClusterInfo(
            instances=cluster.get_instances(),
            last_change_time=cluster.get_last_change_time()
        )
    
    @router.post("/heartbeat/{cluster_id}/{instance_id}")
    async def heartbeat(
        cluster_id: str, 
        instance_id: int,
        authenticated: bool = Depends(authenticate_cluster)
    ) -> HeartbeatResponse:
        """Update heartbeat for an instance."""
        cluster = cluster_registry.get_cluster(cluster_id)
        if not cluster:
            raise ResourceNotFoundError(f"Cluster '{cluster_id}' not found")
        
        if not cluster.update_heartbeat(instance_id):
            raise ResourceNotFoundError(f"Instance {instance_id} not found")
            
        return HeartbeatResponse()
    
    @router.get("/clusters")
    async def get_clusters() -> Dict[str, Any]:
        """
        Get all registered clusters.
        """
        return {
            "clusters": cluster_registry.get_all_cluster_ids(),
            "count": cluster_registry.get_cluster_count()
        }
    
    @router.delete("/instance/{cluster_id}/{instance_id}")
    async def remove_instance(
        cluster_id: str, 
        instance_id: int,
        authenticated: bool = Depends(authenticate_cluster)
    ) -> RemovalResponse:
        """Manually remove an instance."""
        cluster = cluster_registry.get_cluster(cluster_id)
        if not cluster:
            raise ResourceNotFoundError(f"Cluster '{cluster_id}' not found")
        
        # Verify instance exists before removing
        instances = {i.instance_id for i in cluster.get_instances()}
        if instance_id not in instances:
            raise ResourceNotFoundError(f"Instance {instance_id} not found")
        
        cluster.remove_instance(instance_id)
        logger.info(f"Manually removed instance {instance_id} from cluster {cluster_id}")
        
        return RemovalResponse()
    
    @router.get("/health")
    async def health_check() -> HealthStatus:
        """Health check endpoint (no authentication required)."""
        return HealthStatus(
            status="healthy", 
            timestamp=time.time(),
            clusters_configured=len(cluster_registry.get_all_cluster_ids()),
            clusters_active=cluster_registry.get_cluster_count()
        )
    
    return router

def create_app(
    auth_provider: KeyBasedAuthProvider,
    cluster_registry: ClusterRegistry,
    title: str = "ComfyUI Cluster STUN Server",
    description: str = "STUN server for ComfyUI Cluster instance registration",
    version: str = "0.1.0"
) -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Args:
        auth_provider: Provider for authentication
        cluster_registry: Registry for cluster state management
        title: API title
        description: API description
        version: API version
        
    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title=title,
        description=description,
        version=version,
    )
    
    # Set up middleware
    setup_middleware(app)
    
    # Set up exception handlers
    setup_exception_handlers(app)
    
    # Create and include router
    router = create_router(auth_provider, cluster_registry)
    app.include_router(router)
    
    return app