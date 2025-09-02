#!/usr/bin/env python3
"""
TaskFlow - Distributed Task Processing System

Main entry point for running TaskFlow components.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from taskflow.cli.main import cli

if __name__ == "__main__":
    cli()
