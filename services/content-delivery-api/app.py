"""Service 4 – Content Delivery API (public REST gateway, exposed through Ingress).

The only service reachable from the Internet. It
  * aggregates catalog + video metadata + presigned stream URLs for learners,
  * implements CACHE-ASIDE against the partitioned Cache Manager over gRPC,
  * samples cache hits and re-reads the source to detect STALE READS,
  * emits one `content-access-events` Kafka message per cached read
    (hit / miss / stale) for the Flink usage-analytics job,
  * proxies progress writes to the Progress Tracker (REST),
  * serves recommendations and window analytics from DynamoDB,
  * enforces role-based permissions using the User Service (REST)."""

import os

os.environ.setdefault("SERVICE_NAME", "content-delivery-api")

import json
import logging
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any, Callable, Literal, Optional

import boto3
import grpc
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from fastapi import File, Header, HTTPException, Query, UploadFile
from fastapi.staticfiles import StaticFiles
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

import cache_pb2
import cache_pb2_grpc
from common.s24common import (
    EventProducer, ServiceClient, create_app, ensure_topics, env, env_float, env_int, log, logger, now_ms,
)

TOPIC_ACCESS = env("TOPIC_ACCESS_EVENTS", "content-access-events")
STALE_PROBE_RATE = env_float("STALE_PROBE_RATE", 0.1)
TTL = {
    "courses:list": env_int("TTL_COURSE_LIST_SECONDS", 30),
    "course": env_int("TTL_COURSE_SECONDS", 300),
    "lesson": env_int("TTL_LESSON_SECONDS", 300),
    "video": env_int("TTL_VIDEO_SECONDS", 300),
    "stream": env_int("TTL_STREAM_SECONDS", 600),  # presigned URLs live 3600 s
}

CACHE_RESULTS = Counter("cda_cache_lookups_total", "Cache-aside lookups", ["resource", "result"])
STALE_PROBES = Counter("cda_stale_probes_total", "Stale-read probes", ["resource", "result"])
CACHE_LATENCY = Histogram(
    "cda_cache_rpc_seconds", "gRPC latency to the cache manager", ["op"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
)


class State:
    users: ServiceClient
    catalog: ServiceClient
    metadata: ServiceClient
    progress: ServiceClient
    cache_http: ServiceClient
    cache: cache_pb2_grpc.CacheServiceStub
    ddb = None
    producer = EventProducer()
    probes = ThreadPoolExecutor(max_workers=4, thread_name_prefix="stale-probe")
    perm_cache: dict[str, tuple[float, dict]] = {}
    perm_lock = threading.Lock()
    deser = TypeDeserializer()


def startup() -> None:
    State.users = ServiceClient(env("USER_SERVICE_URL", "http://user-service"), "user-service")
    State.catalog = ServiceClient(env("CATALOG_SERVICE_URL", "http://catalog-service"), "catalog-service")
    State.metadata = ServiceClient(env("METADATA_SERVICE_URL", "http://metadata-service"), "metadata-service", timeout=60)
    State.progress = ServiceClient(env("PROGRESS_SERVICE_URL", "http://progress-tracker"), "progress-tracker")
    State.cache_http = ServiceClient(env("CACHE_MANAGER_HTTP_URL", "http://cache-manager"), "cache-manager")
    channel = grpc.insecure_channel(
        env("CACHE_MANAGER_GRPC_TARGET", "dns:///cache-manager-headless:50051"),
        options=[
            ("grpc.lb_policy_name", "round_robin"),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.dns_min_time_between_resolutions_ms", 10000),
        ],
    )
    State.cache = cache_pb2_grpc.CacheServiceStub(channel)
    State.ddb = boto3.client(
        "dynamodb", region_name=env("AWS_REGION", "ap-south-1"),
        config=Config(retries={"max_attempts": 4, "mode": "adaptive"}),
    )
    ensure_topics([TOPIC_ACCESS, env("TOPIC_PROGRESS_EVENTS", "learner-progress-events"),
                   env("TOPIC_CACHE_INVALIDATION", "cache-invalidation"),
                   env("TOPIC_ANALYTICS_RESULTS", "usage-analytics-results")])


def shutdown() -> None:
    State.probes.shutdown(wait=False)
    State.producer.close()


app = create_app("S24 Content Delivery API", (startup, shutdown))

# Browser UI (static/index.html, app.js, style.css) at /ui: same origin as the API, so no CORS setup.
_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_STATIC_DIR):
    app.mount("/ui", StaticFiles(directory=_STATIC_DIR, html=True), name="ui")


# =============================================================================
# Cache-aside over gRPC
# =============================================================================

def _emit_access(resource: str, item_id: str, course_id: str, user_id: str,
                 cache_hit: bool, stale: bool, event_type: str) -> None:
    State.producer.send(TOPIC_ACCESS, item_id, {
        "event_id": uuid.uuid4().hex,
        "event_type": event_type,
        "resource": resource,
        "item_id": item_id,
        "course_id": course_id,
        "user_id": user_id,
        "cache_hit": cache_hit,
        "stale": stale,
        "event_time": now_ms(),
    })


def _probe(key: str, resource: str, item_id: str, course_id: str, user_id: str,
           cached_version: int, loader: Callable[[], tuple[Any, int]]) -> None:
    """Re-read the source of truth; if its version moved on, the hit was stale."""
    try:
        _, fresh_version = loader()
    except HTTPException:
        return
    stale = fresh_version != cached_version
    STALE_PROBES.labels(resource, "stale" if stale else "fresh").inc()
    _emit_access(resource, item_id, course_id, user_id, True, stale, "stale_probe")
    if stale:
        try:
            State.cache.Delete(cache_pb2.DeleteRequest(key=key), timeout=0.3)
        except grpc.RpcError:
            pass
        log(logger, logging.WARNING, "stale read detected", key=key,
            cached_version=cached_version, fresh_version=fresh_version)


def cache_aside(
    key: str, resource: str, item_id: str, loader: Callable[[], tuple[Any, int]],
    user_id: str = "anonymous", course_id: str = "", probe: bool = True,
) -> tuple[Any, dict]:
    read_started = now_ms()
    try:
        t0 = time.perf_counter()
        resp = State.cache.Get(cache_pb2.GetRequest(key=key), timeout=0.3)
        CACHE_LATENCY.labels("get").observe(time.perf_counter() - t0)
    except grpc.RpcError as exc:
        CACHE_RESULTS.labels(resource, "error").inc()
        log(logger, logging.WARNING, "cache unavailable, reading source", key=key, code=str(exc.code()))
        resp = None

    if resp is not None and resp.found:
        CACHE_RESULTS.labels(resource, "hit").inc()
        data = json.loads(resp.value)
        _emit_access(resource, item_id, course_id, user_id, True, False, "access")
        if probe and random.random() < STALE_PROBE_RATE:
            State.probes.submit(_probe, key, resource, item_id, course_id, user_id, resp.version, loader)
        return data, {"cache": "HIT", "key": key, "owner": resp.owner,
                      "served_by": resp.served_by, "age_ms": resp.age_ms, "version": resp.version}

    data, version = loader()
    CACHE_RESULTS.labels(resource, "miss").inc()
    _emit_access(resource, item_id, course_id, user_id, False, False, "access")
    meta = {"cache": "MISS", "key": key, "version": version}
    try:
        t0 = time.perf_counter()
        set_resp = State.cache.Set(cache_pb2.SetRequest(
            key=key, value=json.dumps(data, default=str).encode(), version=version,
            ttl_seconds=TTL.get(resource, 300), read_started_ms=read_started,
        ), timeout=0.3)
        CACHE_LATENCY.labels("set").observe(time.perf_counter() - t0)
        meta.update({"owner": set_resp.owner, "stored": set_resp.stored, "reason": set_resp.reason})
    except grpc.RpcError:
        meta["stored"] = False
    return data, meta


# ----------------------------------------------------------------------- loaders
def load_course_list() -> tuple[Any, int]:
    data = State.catalog.get("/courses", params={"limit": 100})
    return data, int(data.get("version", 0))


def load_course(course_id: str) -> Callable[[], tuple[Any, int]]:
    def _load():
        data = State.catalog.get(f"/courses/{course_id}")
        return data, int(data["version"])
    return _load


def load_lesson(lesson_id: str) -> Callable[[], tuple[Any, int]]:
    def _load():
        data = State.catalog.get(f"/lessons/{lesson_id}")
        return data, int(data["version"])
    return _load


def load_video(lesson_id: str) -> Callable[[], tuple[Any, int]]:
    def _load():
        data = State.metadata.get(f"/videos/{lesson_id}")
        return data, int(data["version"])
    return _load


def load_stream(lesson_id: str) -> Callable[[], tuple[Any, int]]:
    def _load():
        data = State.metadata.get(f"/videos/{lesson_id}/stream-url")
        return data, int(data["version"])
    return _load


# =============================================================================
# Authorization helpers (User Service over REST, memoized for 30 s)
# =============================================================================

def permissions_for(user_id: Optional[str]) -> dict:
    if not user_id:
        raise HTTPException(status_code=401, detail="X-User-Id header required")
    now = time.time()
    with State.perm_lock:
        cached = State.perm_cache.get(user_id)
        if cached and cached[0] > now:
            return cached[1]
    perms = State.users.get(f"/users/{user_id}/permissions")
    with State.perm_lock:
        if len(State.perm_cache) > 10000:
            State.perm_cache.clear()
        State.perm_cache[user_id] = (now + 30, perms)
    return perms


def require(user_id: Optional[str], permission: str) -> dict:
    perms = permissions_for(user_id)
    if permission not in perms["permissions"]:
        raise HTTPException(status_code=403, detail=f"role '{perms['role']}' lacks '{permission}'")
    return perms


def require_self_or(user_id: Optional[str], target_user: str, permission: str) -> dict:
    perms = permissions_for(user_id)
    if user_id != target_user and permission not in perms["permissions"]:
        raise HTTPException(status_code=403, detail="can only access your own data")
    return perms


def _ddb_items(items: list[dict]) -> list[dict]:
    def plain(v):
        if isinstance(v, Decimal):
            return int(v) if v == v.to_integral_value() else float(v)
        if isinstance(v, list):
            return [plain(x) for x in v]
        if isinstance(v, dict):
            return {k: plain(x) for k, x in v.items()}
        return v
    return [plain({k: State.deser.deserialize(v) for k, v in item.items()}) for item in items]


# =============================================================================
# Public API
# =============================================================================

class RegisterBody(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    email: str
    role: Literal["learner", "instructor", "admin"] = "learner"


class CourseBody(BaseModel):
    title: str
    description: str = ""
    category: str = "general"
    level: str = "beginner"


class LessonBody(BaseModel):
    title: str
    summary: str = ""
    position: Optional[int] = None
    duration_seconds: int = 0


class LessonUpdateBody(BaseModel):
    title: Optional[str] = None
    summary: Optional[str] = None
    duration_seconds: Optional[int] = None


class VideoBody(BaseModel):
    title: str
    description: str = ""
    language: str = "en"
    duration_seconds: int = 0
    resolutions: list[str] = Field(default_factory=lambda: ["720p"])
    tags: list[str] = Field(default_factory=list)


class ProgressBody(BaseModel):
    lesson_id: str
    course_id: Optional[str] = None
    event: Literal["play", "pause", "seek", "heartbeat", "complete"] = "heartbeat"
    position_seconds: float = Field(ge=0)
    duration_seconds: float = Field(gt=0)
    watched_delta_seconds: float = Field(default=0, ge=0, le=3600)
    client_ts: Optional[int] = None


@app.get("/")
def root():
    return {
        "service": "S24 Online Video Learning – Content Delivery API",
        "docs": "/docs",
        "ui": "/ui/",
        "endpoints": [
            "POST /api/users", "GET /api/courses", "GET /api/courses/{id}", "GET /api/lessons/{id}",
            "POST /api/progress", "GET /api/users/{id}/progress", "GET /api/users/{id}/recommendations",
            "GET /api/analytics/items/{id}", "GET /api/cache/ring", "GET /api/cache/simulate",
        ],
    }


# ----------------------------------------------------------------------- users
@app.post("/api/users", status_code=201)
def register(body: RegisterBody, x_user_id: Optional[str] = Header(default=None)):
    if body.role == "admin":
        require(x_user_id, "user:admin")
    return State.users.post("/users", json=body.model_dump())


@app.get("/api/users/{user_id}")
def get_user(user_id: str, x_user_id: Optional[str] = Header(default=None)):
    require_self_or(x_user_id, user_id, "user:admin")
    return State.users.get(f"/users/{user_id}")


# ----------------------------------------------------------------------- catalog (cached reads)
@app.get("/api/courses")
def list_courses(x_user_id: Optional[str] = Header(default=None)):
    data, meta = cache_aside("courses:list", "courses:list", "courses:list", load_course_list,
                             user_id=x_user_id or "anonymous", probe=True)
    return {"cache": meta, **data}


@app.get("/api/courses/{course_id}")
def get_course(course_id: str, x_user_id: Optional[str] = Header(default=None)):
    data, meta = cache_aside(f"course:{course_id}", "course", f"course:{course_id}", load_course(course_id),
                             user_id=x_user_id or "anonymous", course_id=course_id)
    return {"cache": meta, "course": data}


@app.get("/api/lessons/{lesson_id}")
def get_lesson(lesson_id: str, x_user_id: Optional[str] = Header(default=None)):
    uid = x_user_id or "anonymous"
    lesson, lesson_meta = cache_aside(f"lesson:{lesson_id}", "lesson", lesson_id, load_lesson(lesson_id), user_id=uid)
    course_id = lesson["course_id"]
    try:
        video, video_meta = cache_aside(f"video:{lesson_id}", "video", lesson_id, load_video(lesson_id),
                                        user_id=uid, course_id=course_id)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        video, video_meta = None, {"cache": "N/A", "reason": "no video metadata yet"}
    stream, stream_meta = None, {"cache": "N/A"}
    if video and video.get("status") == "uploaded":
        stream, stream_meta = cache_aside(f"stream:{lesson_id}", "stream", lesson_id, load_stream(lesson_id),
                                          user_id=uid, course_id=course_id, probe=False)
    progress = None
    if x_user_id:
        try:
            progress = State.progress.get(f"/progress/{x_user_id}/{lesson_id}")
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
    return {
        "lesson": lesson, "video": video, "stream": stream, "progress": progress,
        "cache": {"lesson": lesson_meta, "video": video_meta, "stream": stream_meta},
    }


# ----------------------------------------------------------------------- authoring (instructor)
@app.post("/api/courses", status_code=201)
def create_course(body: CourseBody, x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "course:write")
    return State.catalog.post("/courses", json={**body.model_dump(), "instructor_id": x_user_id})


@app.post("/api/courses/{course_id}/lessons", status_code=201)
def create_lesson(course_id: str, body: LessonBody, x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "course:write")
    return State.catalog.post(f"/courses/{course_id}/lessons", json=body.model_dump())


@app.put("/api/lessons/{lesson_id}")
def update_lesson(lesson_id: str, body: LessonUpdateBody, x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "course:write")
    return State.catalog.put(f"/lessons/{lesson_id}", json=body.model_dump(exclude_none=True))


@app.put("/api/videos/{lesson_id}")
def put_video(lesson_id: str, body: VideoBody, x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "video:upload")
    return State.metadata.put(f"/videos/{lesson_id}", json=body.model_dump())


@app.post("/api/videos/{lesson_id}/upload")
def upload_video(lesson_id: str, file: UploadFile = File(...), x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "video:upload")
    return State.metadata.post(
        f"/videos/{lesson_id}/upload",
        files={"file": (file.filename or "video.mp4", file.file, file.content_type or "video/mp4")},
    )


@app.post("/api/videos/{lesson_id}/upload-url")
def upload_url(lesson_id: str, filename: str = Query(...), content_type: str = Query(default="video/mp4"),
               x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "video:upload")
    return State.metadata.post(f"/videos/{lesson_id}/upload-url",
                               json={"filename": filename, "content_type": content_type})


@app.post("/api/videos/{lesson_id}/upload-complete")
def upload_complete(lesson_id: str, x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "video:upload")
    return State.metadata.post(f"/videos/{lesson_id}/upload-complete")


@app.post("/api/videos/{lesson_id}/subtitles")
def upload_subtitles(lesson_id: str, language: str = Query(...), file: UploadFile = File(...),
                     x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "video:upload")
    return State.metadata.post(
        f"/videos/{lesson_id}/subtitles", params={"language": language},
        files={"file": (file.filename or f"{language}.vtt", file.file, "text/vtt")},
    )


# ----------------------------------------------------------------------- progress
@app.post("/api/progress", status_code=202)
def post_progress(body: ProgressBody, x_user_id: Optional[str] = Header(default=None)):
    if not x_user_id:
        raise HTTPException(status_code=401, detail="X-User-Id header required")
    return State.progress.post("/progress", json={**body.model_dump(), "user_id": x_user_id},
                               expected=(200, 201, 202))


@app.post("/api/progress/batch", status_code=202)
def post_progress_batch(events: list[ProgressBody], x_user_id: Optional[str] = Header(default=None)):
    if not x_user_id:
        raise HTTPException(status_code=401, detail="X-User-Id header required")
    if not 1 <= len(events) <= 100:
        raise HTTPException(status_code=400, detail="batch must contain 1..100 events")
    payload = [{**e.model_dump(), "user_id": x_user_id} for e in events]
    return State.progress.post("/progress/batch", json=payload, expected=(200, 201, 202))


@app.get("/api/users/{user_id}/progress")
def user_progress(user_id: str, completed: Optional[bool] = None,
                  x_user_id: Optional[str] = Header(default=None)):
    require_self_or(x_user_id, user_id, "progress:read:any")
    params = {} if completed is None else {"completed": str(completed).lower()}
    return State.progress.get(f"/progress/{user_id}", params=params)


# ----------------------------------------------------------------------- recommendations (DynamoDB, written by Lambda)
@app.get("/api/users/{user_id}/recommendations")
def recommendations(user_id: str, course_id: Optional[str] = None, limit: int = Query(default=10, ge=1, le=20),
                    x_user_id: Optional[str] = Header(default=None)):
    require_self_or(x_user_id, user_id, "progress:read:any")
    scope = f"COURSE#{course_id}" if course_id else "GLOBAL"
    item = State.ddb.get_item(TableName=env("RECOMMENDATIONS_TABLE", "Recommendations"),
                              Key={"Scope": {"S": scope}}).get("Item")
    ranked = _ddb_items([item])[0].get("Lessons", []) if item else []
    done = {p["lesson_id"] for p in State.progress.get(f"/progress/{user_id}", params={"completed": "true"})["items"]}
    picks = []
    for entry in ranked:
        if entry["lesson_id"] in done:
            continue
        try:
            lesson, _ = cache_aside(f"lesson:{entry['lesson_id']}", "lesson", entry["lesson_id"],
                                    load_lesson(entry["lesson_id"]), user_id=user_id)
        except HTTPException:
            continue
        picks.append({**entry, "title": lesson["title"], "course_id": lesson["course_id"]})
        if len(picks) >= limit:
            break
    return {"user_id": user_id, "scope": scope, "generated_at": item.get("GeneratedAt", {}).get("S") if item else None,
            "items": picks}


# ----------------------------------------------------------------------- analytics (DynamoDB, Flink -> Kafka -> Lambda)
@app.get("/api/analytics/items/{item_id}")
def item_analytics(item_id: str, limit: int = Query(default=30, ge=1, le=500),
                   x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "analytics:read")
    resp = State.ddb.query(
        TableName=env("ANALYTICS_TABLE", "UsageAnalytics"),
        KeyConditionExpression="ItemID = :i", ExpressionAttributeValues={":i": {"S": item_id}},
        ScanIndexForward=False, Limit=limit,
    )
    windows = _ddb_items(resp.get("Items", []))
    totals = {k: sum(w.get(k, 0) for w in windows)
              for k in ("views", "completions", "cache_hits", "cache_misses", "stale_reads", "progress_events")}
    lookups = totals["cache_hits"] + totals["cache_misses"]
    totals["cache_hit_ratio"] = round(totals["cache_hits"] / lookups, 4) if lookups else None
    return {"item_id": item_id, "windows": windows, "totals": totals}


# ----------------------------------------------------------------------- consistent-hashing demo passthrough
@app.get("/api/cache/ring")
def cache_ring():
    return State.cache_http.get("/ring")


@app.get("/api/cache/lookup")
def cache_lookup(key: str):
    return State.cache_http.get("/ring/lookup", params={"key": key})


@app.get("/api/cache/simulate")
def cache_simulate(keys: int = 20000, add: Optional[str] = None, remove: Optional[str] = None,
                   vnodes: Optional[int] = None):
    params = {k: v for k, v in {"keys": keys, "add": add, "remove": remove, "vnodes": vnodes}.items() if v is not None}
    return State.cache_http.get("/ring/simulate", params=params)


@app.delete("/api/cache/{key}")
def cache_evict(key: str, x_user_id: Optional[str] = Header(default=None)):
    require(x_user_id, "cache:admin")
    resp = State.cache.Delete(cache_pb2.DeleteRequest(key=key), timeout=1)
    return {"key": key, "deleted": resp.deleted, "owner": resp.owner}
