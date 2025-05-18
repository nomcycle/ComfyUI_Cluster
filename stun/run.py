#!/usr/bin/env python3
"""
ComfyUI Cluster STUN Server - Wrapper Script

This script provides a convenient way to run the STUN server.
It handles command line arguments and sets up the environment.
"""

import sys

import os
from dotenv import load_dotenv
from src.server import main

# Load environment variables from .env file
load_dotenv()

if __name__ == "__main__":
    sys.exit(main())