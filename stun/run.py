#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Wrapper Script

This script provides a convenient way to run the STUN server.
It handles command line arguments and sets up the environment.
"""

import sys

from src.server import main

if __name__ == "__main__":
    sys.exit(main())