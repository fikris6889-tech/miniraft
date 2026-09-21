#!/usr/bin/env python3
"""Dev convenience script: launch a local N-node MiniRaft cluster as
subprocesses, wait for every node's /health to respond, print each
node's URL, and clean up on Ctrl+C.

Usage:
    python3 cluster_run.py --nodes 3

This is NOT part of the test suite — it's for manually poking at the
cluster with curl while you build/watch Day 2/3/4. As of Day 4
(COMPLETE), the cluster elects a real leader within a couple of
seconds of boot, stays stable, accepts real client writes via
POST /rpc/client_command with op "set"/"delete" that get replicated to
a majority before the leader replies, AND answers real reads (op
"get") on the same endpoint without appending anything to the log —
kill the leader mid-series and the survivors elect a new one that
keeps everything already committed and keeps taking both writes and
reads. See tests/test_replication.py (Day 3) and tests/test_reads.py
(Day 4) for the same behavior pinned down as automated tests, and
smoke_test_day4.py for a scripted end-to-end version of exactly this
kind of manual poking.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from miniraft.config import ClusterConfig  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--base-port", type=int, default=9000)
    args = parser.parse_args()

    node_ids = [f"node{i+1}" for i in range(args.nodes)]
    cluster = ClusterConfig.local_cluster(node_ids, base_port=args.base_port)

    cluster_dict = {
        "nodes": {nid: {"host": a.host, "port": a.port} for nid, a in cluster.nodes.items()}
    }
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(cluster_dict, tmp)
    tmp.close()
    print(f"cluster config written to {tmp.name}")

    procs = []
    for nid in node_ids:
        p = subprocess.Popen(
            [sys.executable, "-m", "miniraft.main", "--id", nid, "--cluster", tmp.name]
        )
        procs.append(p)

    try:
        time.sleep(1.0)
        for nid, addr in cluster.nodes.items():
            try:
                with urllib.request.urlopen(f"{addr.base_url}/health", timeout=1) as resp:
                    print(f"{nid}: {resp.read().decode()}")
            except Exception as exc:
                print(f"{nid}: NOT RESPONDING ({exc})")
        print("\nCluster running. Ctrl+C to stop.")
        print('Try: curl -X POST -d \'{"command":{"op":"set","key":"x","value":"1"}}\' http://127.0.0.1:9000/rpc/client_command')
        print('Then: curl -X POST -d \'{"command":{"op":"get","key":"x"}}\' http://127.0.0.1:9000/rpc/client_command')
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nstopping cluster...")
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(timeout=5)


if __name__ == "__main__":
    main()
