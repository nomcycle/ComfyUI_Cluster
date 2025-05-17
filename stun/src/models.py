#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Data Models

This module contains Pydantic models for data validation with fail-fast principles.
"""

import logging
import time
from typing import List, Optional, Set
from pydantic import BaseModel, Field, field_validator

from .exceptions import ValidationError

logger = logging.getLogger("comfyui-cluster-stun.models")

class InstanceRegistration(BaseModel):
    """
    Data model for instance registration.
    Includes strict validation but raises exceptions instead of exiting.
    """
    address: str
    direct_listen_port: int
    role: str
    instance_id: int
    cluster_id: str
    cluster_key: str
    
    @field_validator('address')
    def address_must_be_valid(cls, v):
        if not v:
            msg = "Address cannot be empty"
            logger.error(msg)
            raise ValueError(msg)
        return v
    
    @field_validator('direct_listen_port')
    def port_must_be_valid(cls, v):
        if not 1 <= v <= 65535:
            msg = f"Invalid port number: {v}. Must be between 1 and 65535"
            logger.error(msg)
            raise ValueError(msg)
        return v
    
    @field_validator('role')
    def role_must_be_valid(cls, v):
        valid_roles: Set[str] = {'leader', 'follower'}
        if v not in valid_roles:
            msg = f"Invalid role: {v}. Must be one of {valid_roles}"
            logger.error(msg)
            raise ValueError(msg)
        return v
    
    @field_validator('instance_id')
    def instance_id_must_be_positive(cls, v):
        if v < 0:
            msg = f"Invalid instance ID: {v}. Must be non-negative"
            logger.error(msg)
            raise ValueError(msg)
        return v
    
    @field_validator('cluster_id')
    def cluster_id_must_not_be_empty(cls, v):
        if not v:
            msg = "Cluster ID cannot be empty"
            logger.error(msg)
            raise ValueError(msg)
        return v
    
    @field_validator('cluster_key')
    def cluster_key_must_be_secure(cls, v):
        if not v:
            msg = "Cluster key cannot be empty"
            logger.error(msg)
            raise ValueError(msg)
        if len(v) < 8:
            msg = "Cluster key is too short (< 8 chars)"
            logger.error(msg)
            raise ValueError(msg)
        return v

class InstanceInfo(BaseModel):
    """
    Data model for instance information returned by the API.
    """
    instance_id: int
    address: str
    direct_listen_port: int
    role: str
    last_seen: float = Field(default_factory=time.time)

class ClusterInfo(BaseModel):
    """
    Data model for cluster information returned by the API.
    """
    instances: List[InstanceInfo]
    last_change_time: float = Field(default_factory=time.time)

class HealthStatus(BaseModel):
    """
    Data model for health check response.
    """
    status: str = "healthy"
    timestamp: float = Field(default_factory=time.time)
    clusters_configured: int
    clusters_active: int

class RegistrationResponse(BaseModel):
    """
    Data model for registration response.
    """
    status: str = "registered"
    instance_count: int
    instance_id: int

class HeartbeatResponse(BaseModel):
    """
    Data model for heartbeat response.
    """
    status: str = "ok"

class RemovalResponse(BaseModel):
    """
    Data model for instance removal response.
    """
    status: str = "removed"