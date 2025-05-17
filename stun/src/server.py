#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server

A simple STUN (Signaling and Tracking Usage for Networks) server for ComfyUI Cluster.
This server provides a central registration point for ComfyUI Cluster instances.

Usage:
    python server.py [--host HOST] [--port PORT] [--log-level LEVEL]
"""

import argparse
import logging
import signal
import sys
from typing import Dict, Any, Optional

import uvicorn
from fastapi import FastAPI

from .api import create_app
from .auth import KeyBasedAuthProvider
from .cluster import ClusterRegistry
from .config import get_config_from_env, ServerConfig
from .exceptions import ConfigurationError

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("comfyui-cluster-stun")

# Shutdown signal handling
def handle_shutdown(signum, frame):
    """Handle shutdown signals by exiting cleanly."""
    logger.info("Received shutdown signal, exiting...")
    sys.exit(0)

def create_components(config: ServerConfig):
    """
    Create core application components.
    
    Args:
        config: Server configuration
        
    Returns:
        Tuple of (auth_provider, cluster_registry)
        
    Raises:
        ConfigurationError: If component creation fails
    """
    auth_provider = KeyBasedAuthProvider(config.cluster_keys)
    cluster_registry = ClusterRegistry()
    
    return auth_provider, cluster_registry

def create_server_app(config: ServerConfig) -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Args:
        config: Server configuration
        
    Returns:
        Configured FastAPI application
        
    Raises:
        ConfigurationError: If app creation fails
    """
    try:
        auth_provider, cluster_registry = create_components(config)
        
        # Create app
        app = create_app(
            auth_provider=auth_provider,
            cluster_registry=cluster_registry
        )
        return app
    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during initialization: {str(e)}", exc_info=True)
        raise ConfigurationError(f"Failed to create application: {str(e)}")

def configure_logging(log_level: str) -> bool:
    """
    Configure logging level.
    
    Args:
        log_level: The logging level to set
        
    Returns:
        True if successful, False otherwise
    """
    try:
        numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_level, int):
            logger.error(f"Invalid log level: {log_level}")
            return False
        logger.setLevel(numeric_level)
        return True
    except Exception as e:
        logger.error(f"Error setting log level: {str(e)}")
        return False

def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

def parse_args(args: Optional[list] = None) -> Dict[str, Any]:
    """
    Parse command line arguments.
    
    Args:
        args: Command line arguments (defaults to sys.argv)
        
    Returns:
        Dictionary of argument name to value
    """
    parser = argparse.ArgumentParser(description="ComfyUI Cluster STUN Server")
    parser.add_argument("--host", help="Host to listen on")
    parser.add_argument("--port", type=int, help="Port to listen on")
    parser.add_argument("--log-level", help="Logging level")
    
    parsed_args = parser.parse_args(args)
    return {k: v for k, v in vars(parsed_args).items() if v is not None}

def run_server(app: FastAPI, config: ServerConfig) -> int:
    """
    Run the server.
    
    Args:
        app: The FastAPI application
        config: Server configuration
        
    Returns:
        Exit code (0 for success, >0 for error)
    """
    try:
        # Print banner
        logger.info("Starting ComfyUI Cluster STUN Server")
        logger.info(f"Listening on {config.host}:{config.port}")
        logger.info(f"Configured authentication for {len(config.cluster_keys)} clusters")
        
        # Start server
        uvicorn.run(
            app, 
            host=config.host, 
            port=config.port,
            log_level=config.log_level.lower()
        )
        return 0
    except Exception as e:
        logger.error(f"Server failed: {str(e)}")
        return 1

def main(args: Optional[list] = None) -> int:
    """
    Main entrypoint for running the server directly.
    
    Args:
        args: Command line arguments (defaults to sys.argv)
    
    Returns:
        Exit code (0 for success, >0 for error)
    """
    try:
        # Parse arguments
        args_dict = parse_args(args)
        
        # Load config
        config = get_config_from_env(args_dict)
        
        # Configure logging
        if not configure_logging(config.log_level):
            return 1
        
        # Setup signal handlers
        setup_signal_handlers()
        
        # Create app
        app = create_server_app(config)
        
        # Run server
        return run_server(app, config)
        
    except ConfigurationError as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())