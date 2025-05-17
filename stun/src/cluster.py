#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Cluster State Management

This module contains the consolidated state management for clusters and instances.
Follows fail-fast principles and single responsibility principle.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple, Set

from .exceptions import ValidationError, ResourceNotFoundError
from .models import InstanceInfo, RegistrationResponse

logger = logging.getLogger("comfyui-cluster-stun.cluster")

class ClusterState:
    """
    In-memory implementation of cluster state management.
    Strictly follows fail-fast principles.
    """
    def __init__(self, cluster_id: str):
        if not cluster_id:
            msg = "Cluster ID cannot be empty"
            logger.error(msg)
            raise ValidationError(msg)
            
        self.cluster_id = cluster_id
        # instance_id -> (address, port, role, last_ping)
        self.instances: Dict[int, Tuple[str, int, str, float]] = {}
        self.last_change_time: float = time.time()
    
    def register_instance(self, instance_id: int, address: str, port: int, role: str) -> None:
        """
        Register or update an instance.
        
        Args:
            instance_id: Unique ID of the instance
            address: Network address of the instance
            port: Port the instance is listening on
            role: Role of the instance (main, worker, etc.)
            
        Raises:
            ValidationError: If any of the parameters are invalid
        """
        if instance_id < 0:
            msg = f"Invalid instance ID: {instance_id}"
            logger.error(msg)
            raise ValidationError(msg)
            
        if not address:
            msg = "Instance address cannot be empty"
            logger.error(msg)
            raise ValidationError(msg)
            
        if not 1 <= port <= 65535:
            msg = f"Invalid port number: {port}"
            logger.error(msg)
            raise ValidationError(msg)
            
        if not role:
            msg = "Instance role cannot be empty"
            logger.error(msg)
            raise ValidationError(msg)
            
        self.instances[instance_id] = (address, port, role, time.time())
        self.last_change_time = time.time()
        logger.info(f"Registered instance {instance_id} in cluster {self.cluster_id}")
    
    def remove_instance(self, instance_id: int) -> None:
        """
        Remove an instance from the cluster.
        
        Args:
            instance_id: ID of the instance to remove
        """
        if instance_id not in self.instances:
            logger.warning(f"Cannot remove non-existent instance: {instance_id}")
            return
            
        del self.instances[instance_id]
        self.last_change_time = time.time()
        logger.info(f"Removed instance {instance_id} from cluster {self.cluster_id}")
    
    def update_heartbeat(self, instance_id: int) -> bool:
        """
        Update the heartbeat time for an instance.
        
        Args:
            instance_id: ID of the instance to update
            
        Returns:
            True if the instance was found and updated, False otherwise
        """
        if instance_id not in self.instances:
            return False
        
        address, port, role, _ = self.instances[instance_id]
        self.instances[instance_id] = (address, port, role, time.time())
        return True
    
    def get_instances(self) -> List[InstanceInfo]:
        """
        Get all registered instances.
        
        Returns:
            List of InstanceInfo objects
        """
        return [
            InstanceInfo(
                instance_id=instance_id,
                address=address,
                direct_listen_port=port,
                role=role,
                last_seen=last_seen
            )
            for instance_id, (address, port, role, last_seen) in self.instances.items()
        ]
    
    def remove_stale_instances(self, max_age_seconds: int = 60) -> List[int]:
        """
        Remove instances that haven't been seen recently.
        
        Args:
            max_age_seconds: Maximum age in seconds before an instance is considered stale
            
        Returns:
            List of instance IDs that were removed
            
        Raises:
            ValidationError: If max_age_seconds is invalid
        """
        if max_age_seconds <= 0:
            msg = f"Invalid max age: {max_age_seconds}"
            logger.error(msg)
            raise ValidationError(msg)
            
        current_time = time.time()
        stale_instances = [
            instance_id for instance_id, (_, _, _, last_seen) in self.instances.items()
            if (current_time - last_seen) > max_age_seconds
        ]
        
        for instance_id in stale_instances:
            self.remove_instance(instance_id)
            
        if stale_instances:
            logger.info(f"Removed {len(stale_instances)} stale instances from cluster {self.cluster_id}")
            
        return stale_instances
    
    def get_last_change_time(self) -> float:
        """
        Get the timestamp of the last state change.
        
        Returns:
            Timestamp as a float
        """
        return self.last_change_time


class ClusterRegistry:
    """
    Registry for managing multiple cluster states.
    Provides a central point for accessing and managing clusters.
    """
    def __init__(self):
        self.clusters: Dict[str, ClusterState] = {}
    
    def get_cluster(self, cluster_id: str) -> Optional[ClusterState]:
        """
        Get a cluster state by ID.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            ClusterState if found, None otherwise
            
        Raises:
            ValidationError: If cluster_id is empty
        """
        if not cluster_id:
            msg = "Cluster ID cannot be empty"
            logger.error(msg)
            raise ValidationError(msg)
            
        return self.clusters.get(cluster_id)
    
    def create_cluster(self, cluster_id: str) -> ClusterState:
        """
        Create a new cluster state.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            Newly created ClusterState
            
        Raises:
            ValidationError: If cluster_id is empty
        """
        if not cluster_id:
            msg = "Cluster ID cannot be empty"
            logger.error(msg)
            raise ValidationError(msg)
            
        if cluster_id in self.clusters:
            logger.warning(f"Cluster {cluster_id} already exists, returning existing instance")
            return self.clusters[cluster_id]
            
        self.clusters[cluster_id] = ClusterState(cluster_id)
        logger.info(f"Created new cluster state for {cluster_id}")
        return self.clusters[cluster_id]
    
    def get_or_create_cluster(self, cluster_id: str) -> ClusterState:
        """
        Get an existing cluster state or create a new one.
        
        Args:
            cluster_id: ID of the cluster
            
        Returns:
            ClusterState for the requested cluster
            
        Raises:
            ValidationError: If cluster_id is empty
        """
        if not cluster_id:
            msg = "Cluster ID cannot be empty"
            logger.error(msg)
            raise ValidationError(msg)
            
        if cluster_id not in self.clusters:
            return self.create_cluster(cluster_id)
        return self.clusters[cluster_id]
    
    def get_all_cluster_ids(self) -> List[str]:
        """
        Get IDs of all registered clusters.
        
        Returns:
            List of cluster IDs
        """
        return list(self.clusters.keys())
    
    def get_cluster_count(self) -> int:
        """
        Get the number of registered clusters.
        
        Returns:
            Number of clusters
        """
        return len(self.clusters)
    
    def cleanup_stale_instances(self, max_age_seconds: int = 60) -> Dict[str, List[int]]:
        """
        Remove stale instances from all clusters.
        
        Args:
            max_age_seconds: Maximum age in seconds before an instance is considered stale
            
        Returns:
            Dictionary mapping cluster IDs to lists of removed instance IDs
            
        Raises:
            ValidationError: If max_age_seconds is invalid
        """
        if max_age_seconds <= 0:
            msg = f"Invalid max age: {max_age_seconds}"
            logger.error(msg)
            raise ValidationError(msg)
            
        result: Dict[str, List[int]] = {}
        for cluster_id, cluster in self.clusters.items():
            removed = cluster.remove_stale_instances(max_age_seconds)
            if removed:
                result[cluster_id] = removed
                
        return result
    
    def register_instance(self, 
                        instance_id: int, 
                        address: str, 
                        port: int, 
                        role: str, 
                        cluster_id: str) -> RegistrationResponse:
        """
        Register an instance with a cluster.
        
        Args:
            instance_id: ID of the instance
            address: Address of the instance
            port: Port the instance is listening on
            role: Role of the instance
            cluster_id: ID of the cluster
            
        Returns:
            Registration response
            
        Raises:
            ValidationError: If any parameters are invalid
        """
        cluster = self.get_or_create_cluster(cluster_id)
        
        cluster.register_instance(
            instance_id=instance_id,
            address=address,
            port=port,
            role=role
        )
        
        logger.info(
            f"Registered instance {instance_id} in cluster "
            f"{cluster_id} at {address}:{port}"
        )
        
        instances = cluster.get_instances()
        return RegistrationResponse(
            status="registered",
            instance_count=len(instances),
            instance_id=instance_id
        )