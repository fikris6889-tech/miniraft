"""Entrypoint: launch a single MiniRaft node process.

Usage:
    python3 -m miniraft.main --id node1 --cluster cluster.json

The process blocks serving RPCs until Ctrl+C. Run one of these per
node (see cluster_run.py for a helper that launches a full local
cluster of N nodes as subprocesses).
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time

from .config import ClusterConfig
from .node import RaftNode


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one MiniRaft node")
    parser.add_argument("--id", required=True, help="this node's id, must match cluster config")
    parser.add_argument("--cluster", required=True, help="path to cluster.json")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    cluster = ClusterConfig.from_file(args.cluster)
    if args.id not in cluster.nodes:
        print(f"error: node id {args.id!r} not found in {args.cluster}", file=sys.stderr)
        sys.exit(1)

    node = RaftNode(args.id, cluster)
    node.start()

    stop = {"flag": False}

    def _handle_sigint(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    try:
        while not stop["flag"]:
            time.sleep(0.2)
    finally:
        node.stop()


if __name__ == "__main__":
    main()
