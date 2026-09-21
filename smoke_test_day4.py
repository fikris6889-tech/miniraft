#!/usr/bin/env python3
"""Manual smoke test for Day 4 (ReadIndex reads) using REAL OS
subprocesses over REAL HTTP — same launch mechanism as cluster_run.py,
matching the project's established "never publish untested code, and
never publish a claim about real behavior you didn't actually capture"
standard from every prior day.

Boots a 3-node cluster, writes a value, reads it back repeatedly while
watching log_length stay flat, then kills the leader and shows the
survivors electing a new leader and taking a write the old leader will
never see. Prints real captured /health + RPC output throughout.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from miniraft.config import ClusterConfig  # noqa: E402


def rpc(base_url, path, payload, timeout=5):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{base_url}{path}", data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def health(base_url, timeout=2):
    with urllib.request.urlopen(f"{base_url}/health", timeout=timeout) as resp:
        return json.loads(resp.read())


def main():
    node_ids = ["node1", "node2", "node3"]
    cluster = ClusterConfig.local_cluster(node_ids, base_port=9500)
    cluster_dict = {"nodes": {nid: {"host": a.host, "port": a.port} for nid, a in cluster.nodes.items()}}
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(cluster_dict, tmp)
    tmp.close()

    procs = {nid: subprocess.Popen([sys.executable, "-m", "miniraft.main", "--id", nid, "--cluster", tmp.name])
             for nid in node_ids}

    try:
        time.sleep(1.5)
        print("=== boot ===")
        for nid, addr in cluster.nodes.items():
            print(f"{nid}: {health(addr.base_url)}")

        leader_id = None
        for _ in range(30):
            for nid, addr in cluster.nodes.items():
                h = health(addr.base_url)
                if h["role"] == "leader":
                    leader_id = nid
                    break
            if leader_id:
                break
            time.sleep(0.2)
        assert leader_id, "no leader elected"
        leader_addr = cluster.address_of(leader_id)
        print(f"\n=== leader elected: {leader_id} ===")
        print(health(leader_addr.base_url))

        print("\n=== write x=100 ===")
        w = rpc(leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": 100}})
        print(w)
        h_after_write = health(leader_addr.base_url)
        print(f"leader /health after write: {h_after_write}")
        log_len_after_write = h_after_write["log_length"]

        print("\n=== 5x get x (must all return 100, log_length must NOT grow) ===")
        for i in range(5):
            r = rpc(leader_addr.base_url, "/rpc/client_command", {"command": {"op": "get", "key": "x"}})
            h = health(leader_addr.base_url)
            print(f"read {i+1}: {r}  |  log_length now: {h['log_length']}")
            assert r["ok"] and r["result"]["value"] == 100
            assert h["log_length"] == log_len_after_write, "a read must never grow the log"
        print("confirmed: 5 reads, log_length held steady at", log_len_after_write)

        print(f"\n=== killing leader {leader_id} ===")
        procs[leader_id].terminate()
        procs[leader_id].wait(timeout=5)

        survivors = [nid for nid in node_ids if nid != leader_id]
        new_leader_id = None
        for _ in range(40):
            for nid in survivors:
                try:
                    h = health(cluster.address_of(nid).base_url)
                    if h["role"] == "leader":
                        new_leader_id = nid
                        break
                except Exception:
                    pass
            if new_leader_id:
                break
            time.sleep(0.2)
        assert new_leader_id, "no new leader elected among survivors"
        print(f"new leader among survivors: {new_leader_id}")
        new_leader_addr = cluster.address_of(new_leader_id)
        print(health(new_leader_addr.base_url))

        print("\n=== new leader accepts a write the dead old leader never saw ===")
        w2 = rpc(new_leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": 200}})
        print(w2)
        r2 = rpc(new_leader_addr.base_url, "/rpc/client_command", {"command": {"op": "get", "key": "x"}})
        print("get x on new leader:", r2)
        assert r2["ok"] and r2["result"]["value"] == 200

        print("\nALL SMOKE ASSERTIONS PASSED")
    finally:
        for nid, p in procs.items():
            if p.poll() is None:
                p.terminate()
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    main()
