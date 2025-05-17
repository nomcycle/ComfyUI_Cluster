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

import uvicorn

from .api import create_app
from .auth import KeyBasedAuthProvider
from .cluster import ClusterRegistry
from .config import get_config_from_env
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

def main():
    """
    Main entrypoint for running the server directly.
    
    Returns:
        Exit code (0 for success, >0 for error)
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="ComfyUI Cluster STUN Server")
    parser.add_argument("--host", help="Host to listen on")
    parser.add_argument("--port", type=int, help="Port to listen on")
    parser.add_argument("--log-level", help="Logging level")
    
    args = parser.parse_args()
    args_dict = {k: v for k, v in vars(args).items() if v is not None}
    
    # Load config - ConfigurationError will cause program to exit
    try:
        config = get_config_from_env(args_dict)
    except ConfigurationError as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error during configuration: {str(e)}", exc_info=True)
        return 1
    
    # Configure log level
    try:
        numeric_level = getattr(logging, config.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            logger.error(f"Invalid log level: {config.log_level}")
            return 1
        logger.setLevel(numeric_level)
    except Exception as e:
        logger.error(f"Error setting log level: {str(e)}")
        return 1
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Create core components
    try:
        auth_provider = KeyBasedAuthProvider(config.cluster_keys)
        cluster_registry = ClusterRegistry()
        
        # Create app
        app = create_app(
            auth_provider=auth_provider,
            cluster_registry=cluster_registry
        )
    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error during initialization: {str(e)}", exc_info=True)
        return 1
    
    # Print banner
    logger.info("Starting ComfyUI Cluster STUN Server")
    logger.info(f"Listening on {config.host}:{config.port}")
    logger.info(f"Configured authentication for {len(config.cluster_keys)} clusters")
    
    # Start server
    try:
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

if __name__ == "__main__":
    sys.exit(main())