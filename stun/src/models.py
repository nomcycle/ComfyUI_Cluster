#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Data Models

This module contains Pydantic models for data validation with fail-fast principles.
"""

import logging
import time
from typing import List, Optional, Set, ClassVar
from pydantic import BaseModel, Field, field_validator, model_validator

from .exceptions import ValidationError

logger = logging.getLogger("comfyui-cluster-stun.models")

def validate_address(address: str) -> str:
    """
    Validate that the address is not empty.
    
    Args:
        address: Network address to validate
        
    Returns:
        Validated address
        
    Raises:
        ValueError: If address is invalid
    """
    if not address:
        msg = "Address cannot be empty"
        logger.error(msg)
        raise ValueError(msg)
    return address

def validate_port(port: int) -> int:
    """
    Validate that the port number is in a valid range.
    
    Args:
        port: Port number to validate
        
    Returns:
        Validated port number
        
    Raises:
        ValueError: If port is invalid
    """
    if not 1 <= port <= 65535:
        msg = f"Invalid port number: {port}. Must be between 1 and 65535"
        logger.error(msg)
        raise ValueError(msg)
    return port

def validate_role(role: str, valid_roles: Optional[Set[str]] = None) -> str:
    """
    Validate that the role is one of the allowed values.
    
    Args:
        role: Role to validate
        valid_roles: Set of valid roles
        
    Returns:
        Validated role
        
    Raises:
        ValueError: If role is invalid
    """
    if valid_roles is None:
        valid_roles = {'leader', 'follower'}
    
    if role not in valid_roles:
        msg = f"Invalid role: {role}. Must be one of {valid_roles}"
        logger.error(msg)
        raise ValueError(msg)
    return role

def validate_instance_id(instance_id: int) -> int:
    """
    Validate that the instance ID is non-negative.
    
    Args:
        instance_id: Instance ID to validate
        
    Returns:
        Validated instance ID
        
    Raises:
        ValueError: If instance ID is invalid
    """
    if instance_id < 0:
        msg = f"Invalid instance ID: {instance_id}. Must be non-negative"
        logger.error(msg)
        raise ValueError(msg)
    return instance_id

def validate_cluster_id(cluster_id: str) -> str:
    """
    Validate that the cluster ID is not empty.
    
    Args:
        cluster_id: Cluster ID to validate
        
    Returns:
        Validated cluster ID
        
    Raises:
        ValueError: If cluster ID is invalid
    """
    if not cluster_id:
        msg = "Cluster ID cannot be empty"
        logger.error(msg)
        raise ValueError(msg)
    return cluster_id

def validate_cluster_key(cluster_key: str, min_length: int = 8) -> str:
    """
    Validate that the cluster key is secure.
    
    Args:
        cluster_key: Cluster key to validate
        min_length: Minimum required length
        
    Returns:
        Validated cluster key
        
    Raises:
        ValueError: If cluster key is invalid
    """
    if not cluster_key:
        msg = "Cluster key cannot be empty"
        logger.error(msg)
        raise ValueError(msg)
    if len(cluster_key) < min_length:
        msg = f"Cluster key is too short (< {min_length} chars)"
        logger.error(msg)
        raise ValueError(msg)
    return cluster_key

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
    
    # Store valid roles as a class variable for easy testing and modification
    valid_roles: ClassVar[Set[str]] = {'leader', 'follower'}
    
    @field_validator('address')
    def address_must_be_valid(cls, v):
        return validate_address(v)
    
    @field_validator('direct_listen_port')
    def port_must_be_valid(cls, v):
        return validate_port(v)
    
    @field_validator('role')
    def role_must_be_valid(cls, v):
        return validate_role(v, cls.valid_roles)
    
    @field_validator('instance_id')
    def instance_id_must_be_positive(cls, v):
        return validate_instance_id(v)
    
    @field_validator('cluster_id')
    def cluster_id_must_not_be_empty(cls, v):
        return validate_cluster_id(v)
    
    @field_validator('cluster_key')
    def cluster_key_must_be_secure(cls, v):
        return validate_cluster_key(v)

class InstanceInfo(BaseModel):
    """
    Data model for instance information returned by the API.
    """
    instance_id: int
    address: str
    direct_listen_port: int
    role: str
    last_seen: float = Field(default_factory=time.time)
    
    @field_validator('instance_id')
    def instance_id_must_be_positive(cls, v):
        return validate_instance_id(v)
    
    @field_validator('address')
    def address_must_be_valid(cls, v):
        return validate_address(v)
    
    @field_validator('direct_listen_port')
    def port_must_be_valid(cls, v):
        return validate_port(v)
    
    @field_validator('role')
    def role_must_be_valid(cls, v):
        return validate_role(v)

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
    
    @field_validator('instance_id')
    def instance_id_must_be_positive(cls, v):
        return validate_instance_id(v)
    
    @field_validator('instance_count')
    def instance_count_must_be_positive(cls, v):
        if v < 0:
            msg = f"Invalid instance count: {v}. Must be non-negative"
            logger.error(msg)
            raise ValueError(msg)
        return v

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