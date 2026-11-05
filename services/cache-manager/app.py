"""Service 5 – Cache Manager (gRPC + REST).

Core concept of scenario S24: DATA PARTITIONING USING CONSISTENT HASHING.

The cache is spread over the replicas of a StatefulSet (cache-manager-0..N-1).
Every key is mapped to its owning replica with a consistent hash ring that
uses MD5 and virtual nodes. Any replica accepts a gRPC call; non-owners
forward it to the owner, so callers need no knowledge of the partitioning.

Behaviour implemented on top of the partitioned store:
  * cache-aside  – clients Get, on miss read the source of truth and Set
  * expiry       – per-key TTL, lazy expiry on read + periodic sweeper, LRU bound
  * invalidation – Kafka topic `cache-invalidation` (broadcast to every replica;
                   the owner drops the key and leaves a short tombstone that
                   rejects late Sets whose read began before the invalidation)

Sections:
  1. Consistent hash ring (MD5, virtual nodes, preference lists, rebalance simulation)
  2. Shard store (TTL + LRU + tombstones)
  3. Membership + gRPC servicer with peer forwarding
  4. Background workers (sweeper, Kafka invalidation consumer)
  5. REST API (health, metrics, ring inspection/simulation, REST mirror)
"""

import os

os.environ.setdefault("SERVICE_NAME", "cache-manager")

import bisect
import hashlib
import json
import logging
import random
import threading
import time
import uuid
from collections import Counter as TallyCounter
from collections import OrderedDict
from concurrent import futures
from dataclasses import dataclass
from typing import Iterable, Optional

import grpc
from fastapi import HTTPException, Query
from prometheus_client import Counter, Gauge
from pydantic import BaseModel, Field

import cache_pb2
import cache_pb2_grpc
from common.s24common import KAFKA_MESSAGES, create_app, env, env_int, kafka_bootstrap, log, logger, now_ms


# =============================================================================
# 1. CONSISTENT HASH RING (MD5 + VIRTUAL NODES)
# =============================================================================

"""Each physical node is placed on a 64-bit ring `vnodes` times at positions
MD5("<node>#vn<i>")[:8]. A key belongs to the first virtual node clockwise
from MD5(key)[:8]. Adding or removing one of N nodes remaps only ~1/N of the
keys, whereas `hash(key) % N` remaps ~(N-1)/N of them; virtual nodes even out
the share of the hash space each physical node owns."""

RING_SIZE = 2 ** 64


def md5_hash(value: str) -> int:
    """First 8 bytes of the MD5 digest as an unsigned 64-bit integer."""
    return int.from_bytes(hashlib.md5(value.encode("utf-8")).digest()[:8], "big")


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str] = (), vnodes: int = 160) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._lock = threading.RLock()
        self._hashes: list[int] = []            # sorted vnode positions
        self._owners: dict[int, tuple[str, int]] = {}  # position -> (node, vnode index)
        self._nodes: set[str] = set()
        for node in nodes:
            self.add_node(node)

    # ------------------------------------------------------------------ membership
    @property
    def nodes(self) -> list[str]:
        with self._lock:
            return sorted(self._nodes)

    def add_node(self, node: str) -> int:
        """Place `vnodes` virtual nodes for `node`; returns how many were placed."""
        with self._lock:
            if node in self._nodes:
                return 0
            placed = 0
            new_positions = []
            for i in range(self.vnodes):
                h = md5_hash(f"{node}#vn{i}")
                if h in self._owners:  # astronomically rare 64-bit collision: skip this vnode
                    continue
                self._owners[h] = (node, i)
                new_positions.append(h)
                placed += 1
            self._hashes = sorted(self._hashes + new_positions)
            self._nodes.add(node)
            return placed

    def remove_node(self, node: str) -> int:
        with self._lock:
            if node not in self._nodes:
                return 0
            removed = [h for h, (n, _) in self._owners.items() if n == node]
            for h in removed:
                del self._owners[h]
            removed_set = set(removed)
            self._hashes = [h for h in self._hashes if h not in removed_set]
            self._nodes.discard(node)
            return len(removed)

    # ------------------------------------------------------------------ lookup
    def _index_for(self, key_hash: int) -> int:
        idx = bisect.bisect_left(self._hashes, key_hash)
        return 0 if idx == len(self._hashes) else idx

    def get_node(self, key: str) -> Optional[str]:
        with self._lock:
            if not self._hashes:
                return None
            return self._owners[self._hashes[self._index_for(md5_hash(key))]][0]

    def lookup(self, key: str) -> dict:
        """Full routing detail for one key (used by the demo endpoints)."""
        with self._lock:
            key_hash = md5_hash(key)
            if not self._hashes:
                return {"key": key, "key_hash": key_hash, "owner": None}
            vnode_hash = self._hashes[self._index_for(key_hash)]
            node, vidx = self._owners[vnode_hash]
            return {
                "key": key,
                "key_hash": key_hash,
                "key_position_pct": round(key_hash / RING_SIZE * 100, 6),
                "vnode": f"{node}#vn{vidx}",
                "vnode_hash": vnode_hash,
                "owner": node,
                "replicas": self.get_nodes(key, min(3, len(self._nodes))),
            }

    def get_nodes(self, key: str, count: int) -> list[str]:
        """Preference list: the first `count` distinct physical nodes clockwise."""
        with self._lock:
            if not self._hashes:
                return []
            count = min(count, len(self._nodes))
            result: list[str] = []
            idx = self._index_for(md5_hash(key))
            for step in range(len(self._hashes)):
                node = self._owners[self._hashes[(idx + step) % len(self._hashes)]][0]
                if node not in result:
                    result.append(node)
                    if len(result) == count:
                        break
            return result

    # ------------------------------------------------------------------ analysis
    def ownership(self) -> dict[str, float]:
        """Fraction of the 64-bit hash space owned by each physical node."""
        with self._lock:
            if not self._hashes:
                return {}
            owned: TallyCounter = TallyCounter()
            prev = self._hashes[-1] - RING_SIZE  # arc that wraps around zero
            for h in self._hashes:
                owned[self._owners[h][0]] += h - prev
                prev = h
            return {node: round(owned[node] / RING_SIZE, 6) for node in sorted(self._nodes)}

    def vnode_positions(self, node: Optional[str] = None, limit: int = 50) -> list[dict]:
        with self._lock:
            out = []
            for h in self._hashes:
                n, i = self._owners[h]
                if node is None or n == node:
                    out.append({"vnode": f"{n}#vn{i}", "hash": h, "position_pct": round(h / RING_SIZE * 100, 4)})
                    if len(out) >= limit:
                        break
            return out

    def clone(self) -> "ConsistentHashRing":
        with self._lock:
            copy = ConsistentHashRing(vnodes=self.vnodes)
            copy._hashes = list(self._hashes)
            copy._owners = dict(self._owners)
            copy._nodes = set(self._nodes)
            return copy


def simulate_membership_change(
    ring: ConsistentHashRing,
    sample_keys: list[str],
    add: Optional[str] = None,
    remove: Optional[str] = None,
) -> dict:
    """Compare key movement for consistent hashing vs. naive modulo hashing."""
    before_nodes = ring.nodes
    after = ring.clone()
    if add:
        after.add_node(add)
    if remove:
        after.remove_node(remove)
    after_nodes = after.nodes
    if not before_nodes or not after_nodes:
        raise ValueError("ring must contain nodes before and after the change")

    moved_consistent = 0
    moved_modulo = 0
    distribution_before: TallyCounter = TallyCounter()
    distribution_after: TallyCounter = TallyCounter()
    for key in sample_keys:
        a, b = ring.get_node(key), after.get_node(key)
        distribution_before[a] += 1
        distribution_after[b] += 1
        if a != b:
            moved_consistent += 1
        h = md5_hash(key)
        if before_nodes[h % len(before_nodes)] != after_nodes[h % len(after_nodes)]:
            moved_modulo += 1

    total = len(sample_keys)
    n_before, n_after = len(before_nodes), len(after_nodes)
    ideal = abs(n_after - n_before) / max(n_after, n_before)
    return {
        "sample_keys": total,
        "nodes_before": before_nodes,
        "nodes_after": after_nodes,
        "vnodes_per_node": ring.vnodes,
        "consistent_hashing": {
            "moved_keys": moved_consistent,
            "moved_pct": round(moved_consistent / total * 100, 2),
            "ideal_moved_pct": round(ideal * 100, 2),
        },
        "modulo_hashing": {
            "moved_keys": moved_modulo,
            "moved_pct": round(moved_modulo / total * 100, 2),
        },
        "keys_per_node_before": dict(sorted(distribution_before.items())),
        "keys_per_node_after": dict(sorted(distribution_after.items())),
        "ownership_after": after.ownership(),
    }


# =============================================================================
# 2. SHARD STORE – the local partition held by this replica (cache-aside target)
# =============================================================================

@dataclass
class Entry:
    value: bytes
    version: int
    created_ms: int
    expires_ms: int


class ShardStore:
    """Bounded LRU map with per-entry TTL, lazy + periodic expiry and
    invalidation tombstones (to reject stale cache-aside writes)."""

    def __init__(self, max_entries: int, tombstone_ms: int) -> None:
        self.max_entries = max_entries
        self.tombstone_ms = tombstone_ms
        self._data: "OrderedDict[str, Entry]" = OrderedDict()
        self._tombstones: dict[str, int] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Entry]:
        now = now_ms()
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if entry.expires_ms <= now:
                del self._data[key]
                CACHE_EXPIRED.labels("lazy").inc()
                return None
            self._data.move_to_end(key)
            return entry

    def set(self, key: str, value: bytes, version: int, ttl_seconds: int, read_started_ms: int) -> tuple[bool, str]:
        now = now_ms()
        with self._lock:
            invalidated_at = self._tombstones.get(key)
            if invalidated_at is not None:
                if now - invalidated_at > self.tombstone_ms:
                    del self._tombstones[key]
                elif read_started_ms and read_started_ms <= invalidated_at:
                    return False, "rejected: value was read before the latest invalidation"
            current = self._data.get(key)
            if current is not None and current.expires_ms > now and current.version > version:
                return False, f"rejected: cached version {current.version} is newer than {version}"
            self._data[key] = Entry(value, version, now, now + ttl_seconds * 1000)
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)
                CACHE_EVICTIONS.inc()
            return True, "stored"

    def delete(self, key: str) -> bool:
        with self._lock:
            self._tombstones[key] = now_ms()
            return self._data.pop(key, None) is not None

    def sweep(self) -> int:
        now = now_ms()
        with self._lock:
            expired = [k for k, e in self._data.items() if e.expires_ms <= now]
            for k in expired:
                del self._data[k]
            stale_tombstones = [k for k, t in self._tombstones.items() if now - t > self.tombstone_ms]
            for k in stale_tombstones:
                del self._tombstones[k]
        if expired:
            CACHE_EXPIRED.labels("sweeper").inc(len(expired))
        return len(expired)

    def stats(self) -> dict:
        with self._lock:
            return {
                "entries": len(self._data),
                "max_entries": self.max_entries,
                "tombstones": len(self._tombstones),
                "approx_bytes": sum(len(e.value) for e in self._data.values()),
            }

    def keys(self, limit: int) -> list[str]:
        with self._lock:
            return list(self._data.keys())[-limit:]


# =============================================================================
# 3. CLUSTER MEMBERSHIP + gRPC SERVER (with forwarding to the owning replica)
# =============================================================================

NAMESPACE = env("POD_NAMESPACE", "s24")
POD_NAME = env("POD_NAME", "cache-manager-0")
STATEFULSET = env("STATEFULSET_NAME", "cache-manager")
HEADLESS = env("HEADLESS_SERVICE", "cache-manager-headless")
GRPC_PORT = env_int("GRPC_PORT", 50051)
REPLICAS = env_int("CACHE_REPLICAS", 3)
VNODES = env_int("VIRTUAL_NODES", 160)
DEFAULT_TTL = env_int("DEFAULT_TTL_SECONDS", 300)
MAX_TTL = env_int("MAX_TTL_SECONDS", 3600)


def node_address(ordinal: int) -> str:
    return f"{STATEFULSET}-{ordinal}.{HEADLESS}.{NAMESPACE}.svc.cluster.local:{GRPC_PORT}"


# On Kubernetes membership is derived from the StatefulSet ordinals. For local runs
# (docker compose / tests) CACHE_PEER_ADDRESSES + CACHE_SELF_ADDRESS override it.
_PEER_OVERRIDE = [a.strip() for a in env("CACHE_PEER_ADDRESSES", "").split(",") if a.strip()]
if _PEER_OVERRIDE:
    MEMBERS = _PEER_OVERRIDE
    SELF = env("CACHE_SELF_ADDRESS", _PEER_OVERRIDE[0])
else:
    MEMBERS = [node_address(i) for i in range(REPLICAS)]
    _ordinal = POD_NAME.rsplit("-", 1)[-1]
    SELF = node_address(int(_ordinal)) if _ordinal.isdigit() else POD_NAME
RING = ConsistentHashRing(MEMBERS, vnodes=VNODES)
STORE = ShardStore(env_int("MAX_ENTRIES", 50000), env_int("TOMBSTONE_MS", 10000))

CACHE_OPS = Counter("cache_requests_total", "Cache operations", ["op", "result"])
CACHE_EVICTIONS = Counter("cache_evictions_total", "LRU evictions")
CACHE_EXPIRED = Counter("cache_expirations_total", "TTL expirations", ["mode"])
CACHE_FORWARDS = Counter("cache_forwards_total", "Requests forwarded to the owning replica", ["op", "result"])
CACHE_INVALIDATIONS = Counter("cache_invalidations_total", "Keys invalidated from Kafka", ["result"])
CACHE_ENTRIES = Gauge("cache_entries", "Entries held by this replica")
RING_OWNERSHIP = Gauge("cache_ring_ownership_ratio", "Fraction of hash space owned", ["node"])
RING_VNODES = Gauge("cache_ring_vnodes", "Virtual nodes per physical node")

for _node, _share in RING.ownership().items():
    RING_OWNERSHIP.labels(_node.split(".")[0]).set(_share)
RING_VNODES.set(VNODES)


class Peers:
    _stubs: dict[str, cache_pb2_grpc.CacheServiceStub] = {}
    _lock = threading.Lock()

    @classmethod
    def stub(cls, address: str) -> cache_pb2_grpc.CacheServiceStub:
        with cls._lock:
            stub = cls._stubs.get(address)
            if stub is None:
                channel = grpc.insecure_channel(address, options=[("grpc.keepalive_time_ms", 30000)])
                stub = cache_pb2_grpc.CacheServiceStub(channel)
                cls._stubs[address] = stub
            return stub


def short(address: str) -> str:
    return address.split(".")[0]


class CacheServicer(cache_pb2_grpc.CacheServiceServicer):
    FORWARD_TIMEOUT = 0.5

    def Get(self, request, context):
        owner = RING.get_node(request.key)
        if owner != SELF and not request.forwarded:
            try:
                resp = Peers.stub(owner).Get(
                    cache_pb2.GetRequest(key=request.key, forwarded=True), timeout=self.FORWARD_TIMEOUT
                )
                CACHE_FORWARDS.labels("get", "ok").inc()
                return resp
            except grpc.RpcError as exc:
                CACHE_FORWARDS.labels("get", "error").inc()
                log(logger, logging.WARNING, "forward failed", op="get", owner=owner, code=str(exc.code()))
                CACHE_OPS.labels("get", "miss").inc()
                return cache_pb2.GetResponse(found=False, owner=short(owner), served_by=short(SELF))
        entry = STORE.get(request.key)
        if entry is None:
            CACHE_OPS.labels("get", "miss").inc()
            return cache_pb2.GetResponse(found=False, owner=short(owner), served_by=short(SELF))
        CACHE_OPS.labels("get", "hit").inc()
        return cache_pb2.GetResponse(
            found=True, value=entry.value, version=entry.version, expires_at_ms=entry.expires_ms,
            age_ms=now_ms() - entry.created_ms, owner=short(owner), served_by=short(SELF),
        )

    def Set(self, request, context):
        owner = RING.get_node(request.key)
        if owner != SELF and not request.forwarded:
            forwarded = cache_pb2.SetRequest()
            forwarded.CopyFrom(request)
            forwarded.forwarded = True
            try:
                resp = Peers.stub(owner).Set(forwarded, timeout=self.FORWARD_TIMEOUT)
                CACHE_FORWARDS.labels("set", "ok").inc()
                return resp
            except grpc.RpcError as exc:
                CACHE_FORWARDS.labels("set", "error").inc()
                return cache_pb2.SetResponse(stored=False, owner=short(owner), reason=f"owner unreachable: {exc.code()}")
        ttl = request.ttl_seconds if request.ttl_seconds > 0 else DEFAULT_TTL
        stored, reason = STORE.set(request.key, request.value, request.version, min(ttl, MAX_TTL), request.read_started_ms)
        CACHE_OPS.labels("set", "stored" if stored else "rejected").inc()
        CACHE_ENTRIES.set(STORE.stats()["entries"])
        return cache_pb2.SetResponse(stored=stored, owner=short(owner), reason=reason)

    def Delete(self, request, context):
        owner = RING.get_node(request.key)
        if owner != SELF and not request.forwarded:
            try:
                resp = Peers.stub(owner).Delete(
                    cache_pb2.DeleteRequest(key=request.key, forwarded=True), timeout=self.FORWARD_TIMEOUT
                )
                CACHE_FORWARDS.labels("delete", "ok").inc()
                return resp
            except grpc.RpcError:
                CACHE_FORWARDS.labels("delete", "error").inc()
                return cache_pb2.DeleteResponse(deleted=False, owner=short(owner))
        deleted = STORE.delete(request.key)
        CACHE_OPS.labels("delete", "deleted" if deleted else "absent").inc()
        return cache_pb2.DeleteResponse(deleted=deleted, owner=short(owner))

    def Lookup(self, request, context):
        info = RING.lookup(request.key)
        return cache_pb2.LookupResponse(
            owner=short(info["owner"]), key_hash=info["key_hash"],
            vnode_hash=info["vnode_hash"], vnode=short(info["vnode"].split("#")[0]) + "#" + info["vnode"].split("#")[1],
        )


# =============================================================================
# 4. BACKGROUND WORKERS – TTL sweeper and Kafka invalidation consumer
# =============================================================================

class Workers:
    stop = threading.Event()
    grpc_server: Optional[grpc.Server] = None
    consumer_ready = threading.Event()


def sweeper_loop() -> None:
    interval = env_int("SWEEP_INTERVAL_SECONDS", 15)
    while not Workers.stop.wait(interval):
        removed = STORE.sweep()
        CACHE_ENTRIES.set(STORE.stats()["entries"])
        if removed:
            log(logger, logging.INFO, "expired entries swept", removed=removed)


def invalidation_loop() -> None:
    """Every replica consumes all invalidation events (unique group id = broadcast)
    and drops the keys that the ring assigns to it."""
    bootstrap = kafka_bootstrap()
    topic = env("TOPIC_CACHE_INVALIDATION", "cache-invalidation")
    if not bootstrap:
        log(logger, logging.WARNING, "kafka not configured; invalidation consumer disabled")
        return
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"cache-manager-{POD_NAME}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "client.id": POD_NAME,
    })
    while not Workers.stop.is_set():
        try:
            consumer.subscribe([topic])
            Workers.consumer_ready.set()
            break
        except KafkaException as exc:
            log(logger, logging.WARNING, "subscribe failed, retrying", error=str(exc))
            time.sleep(5)
    while not Workers.stop.is_set():
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if "UNKNOWN_TOPIC" not in str(msg.error()):
                log(logger, logging.WARNING, "consumer error", error=str(msg.error()))
            continue
        KAFKA_MESSAGES.labels("cache-manager", topic, "consume", "ok").inc()
        try:
            event = json.loads(msg.value())
        except ValueError:
            KAFKA_MESSAGES.labels("cache-manager", topic, "consume", "bad_json").inc()
            continue
        for key in event.get("keys", []):
            if RING.get_node(key) == SELF:
                deleted = STORE.delete(key)
                CACHE_INVALIDATIONS.labels("deleted" if deleted else "absent").inc()
                log(logger, logging.INFO, "key invalidated", key=key, deleted=deleted,
                    reason=event.get("reason"), source=event.get("source"))
    consumer.close()


def start_background() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=env_int("GRPC_WORKERS", 32)))
    cache_pb2_grpc.add_CacheServiceServicer_to_server(CacheServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    Workers.grpc_server = server
    threading.Thread(target=sweeper_loop, name="sweeper", daemon=True).start()
    threading.Thread(target=invalidation_loop, name="invalidation", daemon=True).start()
    log(logger, logging.INFO, "cache node online", self=SELF, ring_nodes=RING.nodes, vnodes=VNODES,
        ownership=RING.ownership())


def stop_background() -> None:
    Workers.stop.set()
    if Workers.grpc_server is not None:
        Workers.grpc_server.stop(grace=5)


# =============================================================================
# 5. REST API – health/metrics, demo endpoints for the ring, REST mirror of the cache
# =============================================================================

app = create_app("S24 Cache Manager", (start_background, stop_background),
                 readiness=lambda: Workers.grpc_server is not None)
SELF_STUB_TARGET = f"localhost:{GRPC_PORT}"


def local_stub() -> cache_pb2_grpc.CacheServiceStub:
    return Peers.stub(SELF_STUB_TARGET)


class CachePut(BaseModel):
    value: dict | list | str | int | float | bool
    version: int = 0
    ttl_seconds: int = Field(default=0, ge=0, le=86400)


@app.get("/ring")
def ring_state():
    return {
        "self": short(SELF),
        "nodes": [short(n) for n in RING.nodes],
        "vnodes_per_node": RING.vnodes,
        "total_vnodes": RING.vnodes * len(RING.nodes),
        "hash_function": "md5 (first 64 bits)",
        "ownership": {short(k): v for k, v in RING.ownership().items()},
    }


@app.get("/ring/lookup")
def ring_lookup(key: str = Query(min_length=1)):
    info = RING.lookup(key)
    info["owner"] = short(info["owner"])
    info["vnode"] = short(info["vnode"].split("#")[0]) + "#" + info["vnode"].split("#")[1]
    info["replicas"] = [short(r) for r in info["replicas"]]
    return info


@app.get("/ring/vnodes")
def ring_vnodes(node: Optional[str] = None, limit: int = Query(default=50, ge=1, le=1000)):
    full = next((n for n in RING.nodes if short(n) == node), None) if node else None
    if node and full is None:
        raise HTTPException(status_code=404, detail="unknown node")
    positions = RING.vnode_positions(full, limit)
    for p in positions:
        name, idx = p["vnode"].split("#")
        p["vnode"] = f"{short(name)}#{idx}"
    return {"node": node, "positions": positions}


@app.get("/ring/simulate")
def ring_simulate(
    keys: int = Query(default=20000, ge=100, le=200000),
    add: Optional[str] = Query(default=None, description="node to add, e.g. cache-manager-3"),
    remove: Optional[str] = Query(default=None, description="node to remove, e.g. cache-manager-2"),
    vnodes: Optional[int] = Query(default=None, ge=1, le=2000),
):
    """Offline what-if: how many keys move if a node joins/leaves (consistent vs modulo)."""
    if not add and not remove:
        add = f"{STATEFULSET}-{REPLICAS}"
    base = ConsistentHashRing(RING.nodes, vnodes=vnodes) if vnodes else RING

    def full_name(name: Optional[str]) -> Optional[str]:
        if name is None:
            return None
        if "." in name:
            return name
        ordinal = name.rsplit("-", 1)[-1]
        return node_address(int(ordinal)) if ordinal.isdigit() else name

    sample = [f"lesson:{uuid.UUID(int=random.getrandbits(128))}" for _ in range(keys)]
    result = simulate_membership_change(base, sample, add=full_name(add), remove=full_name(remove))
    result["nodes_before"] = [short(n) for n in result["nodes_before"]]
    result["nodes_after"] = [short(n) for n in result["nodes_after"]]
    for field in ("keys_per_node_before", "keys_per_node_after", "ownership_after"):
        result[field] = {short(k): v for k, v in result[field].items()}
    return result


@app.get("/cache/stats")
def cache_stats():
    return {"node": short(SELF), **STORE.stats(), "default_ttl_seconds": DEFAULT_TTL}


@app.get("/cache/keys")
def cache_keys(limit: int = Query(default=50, ge=1, le=1000)):
    return {"node": short(SELF), "keys": STORE.keys(limit)}


@app.get("/cache/{key}")
def rest_get(key: str):
    resp = local_stub().Get(cache_pb2.GetRequest(key=key), timeout=1)
    if not resp.found:
        raise HTTPException(status_code=404, detail={"found": False, "owner": resp.owner, "served_by": resp.served_by})
    return {
        "key": key, "value": json.loads(resp.value), "version": resp.version, "age_ms": resp.age_ms,
        "expires_at_ms": resp.expires_at_ms, "owner": resp.owner, "served_by": resp.served_by,
    }


@app.put("/cache/{key}")
def rest_put(key: str, body: CachePut):
    resp = local_stub().Set(cache_pb2.SetRequest(
        key=key, value=json.dumps(body.value).encode(), version=body.version,
        ttl_seconds=body.ttl_seconds, read_started_ms=now_ms(),
    ), timeout=1)
    return {"key": key, "stored": resp.stored, "owner": resp.owner, "reason": resp.reason}


@app.delete("/cache/{key}")
def rest_delete(key: str):
    resp = local_stub().Delete(cache_pb2.DeleteRequest(key=key), timeout=1)
    return {"key": key, "deleted": resp.deleted, "owner": resp.owner}
