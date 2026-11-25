"""Command line: ``python -m pipeline worker``."""

import asyncio
import sys

from pipeline.config import Settings
from pipeline.main import run_worker

COMMANDS = {"worker": run_worker}


def main() -> None:
    """Run the service named on the command line."""
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit(f"usage: python -m pipeline {{{'|'.join(COMMANDS)}}}")
    asyncio.run(COMMANDS[sys.argv[1]](Settings()))


if __name__ == "__main__":
    main()
