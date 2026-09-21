"""Cluster configuration for MiniRaft.

A MiniRaft cluster is a fixed set of nodes, each identified by a
string node_id and reachable at host:port. This module is pure data
plumbing — fully implemented, nothing to build here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class NodeAddress:
    host: str
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass(frozen=True)
class ClusterConfig:
    """Maps every node_id in the cluster to its address."""

    nodes: Dict[str, NodeAddress] = field(default_factory=dict)

    def peers_of(self, node_id: str) -> Dict[str, NodeAddress]:
        """Every node in the cluster except node_id itself."""
        return {nid: addr for nid, addr in self.nodes.items() if nid != node_id}

    def address_of(self, node_id: str) -> NodeAddress:
        return self.nodes[node_id]

    @staticmethod
    def from_dict(raw: dict) -> "ClusterConfig":
        nodes = {
            nid: NodeAddress(host=entry["host"], port=int(entry["port"]))
            for nid, entry in raw["nodes"].items()
        }
        return ClusterConfig(nodes=nodes)

    @staticmethod
    def from_file(path: str) -> "ClusterConfig":
        with open(path, "r", encoding="utf-8") as f:
            return ClusterConfig.from_dict(json.load(f))

    @staticmethod
    def local_cluster(node_ids, base_port: int = 9000) -> "ClusterConfig":
        """Convenience: a cluster of N nodes all on localhost, sequential ports.
        Handy for local testing (see cluster_run.py)."""
        nodes = {
            nid: NodeAddress(host="127.0.0.1", port=base_port + i)
            for i, nid in enumerate(node_ids)
        }
        return ClusterConfig(nodes=nodes)
