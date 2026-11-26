"""Command line: ``python -m pipeline {worker|dispatcher|replay}``."""

import argparse
import asyncio
from uuid import UUID

from pipeline.config import Settings
from pipeline.main import run_dispatcher, run_replay, run_worker


def main() -> None:
    """Run the service or operator command named on the command line."""
    parser = argparse.ArgumentParser(prog="python -m pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("worker", help="consume and process requests")
    commands.add_parser("dispatcher", help="sweeper, webhooks and health gauges")
    replay = commands.add_parser("replay", help="resume needs_human requests after the cause is fixed")
    replay.add_argument("--limit", type=int, default=1000)
    replay.add_argument("--id", dest="ids", type=UUID, action="append", help="only these request ids")
    args = parser.parse_args()

    settings = Settings()
    if args.command == "worker":
        asyncio.run(run_worker(settings))
    elif args.command == "dispatcher":
        asyncio.run(run_dispatcher(settings))
    else:
        replayed = asyncio.run(run_replay(settings, args.limit, args.ids))
        print(f"replayed {len(replayed)} request(s)")
        for request_id in replayed:
            print(request_id)


if __name__ == "__main__":
    main()
