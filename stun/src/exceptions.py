#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Exceptions Module

This module defines custom exceptions used throughout the application
to provide a consistent approach to error handling.
"""

class ComfyClusterError(Exception):
    """Base exception for all ComfyUI Cluster errors."""
    pass


class ConfigurationError(ComfyClusterError):
    """
    Raised when there is an error in the application configuration.
    This is a fatal error that should cause the application to exit.
    """
    pass


class ValidationError(ComfyClusterError):
    """
    Raised when incoming data fails validation.
    This is a non-fatal error that should be handled gracefully.
    """
    pass


class ResourceNotFoundError(ComfyClusterError):
    """
    Raised when a requested resource (cluster, instance) is not found.
    This is a non-fatal error that should be handled gracefully.
    """
    pass


class AuthenticationError(ComfyClusterError):
    """
    Raised when authentication fails.
    This is a non-fatal error that should be handled gracefully.
    """
    pass