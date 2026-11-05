"""Shared runtime for S24 services: JSON logging, Prometheus RED metrics,
FastAPI app factory, Kafka producer/admin helpers, PostgreSQL pool and an
HTTP client for service-to-service REST calls."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Callable, Iterable, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

SERVICE_NAME = os.getenv("SERVICE_NAME", "s24-service")


# --------------------------------------------------------------------------- utils
def now_ms() -> int:
    return int(time.time() * 1000)


def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"required environment variable {name} is not set")
    return value if value is not None else ""


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


# --------------------------------------------------------------------------- logging
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "service": SERVICE_NAME,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging() -> logging.Logger:
    root = logging.getLogger()
    if not any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root.handlers = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    for noisy in ("uvicorn.access", "botocore", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(SERVICE_NAME)


def log(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    logger.log(level, msg, extra={"fields": fields})


logger = setup_logging()


# --------------------------------------------------------------------------- metrics
HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests handled", ["service", "method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
HTTP_IN_FLIGHT = Gauge("http_requests_in_flight", "In-flight HTTP requests", ["service"])
DOWNSTREAM_CALLS = Counter(
    "downstream_requests_total", "Outgoing REST calls", ["service", "target", "status"]
)
KAFKA_MESSAGES = Counter(
    "kafka_messages_total", "Kafka messages produced/consumed", ["service", "topic", "direction", "result"]
)

_SKIP_METRICS_ROUTES = {"/metrics", "/healthz", "/readyz"}


def create_app(
    title: str,
    lifespan_hooks: Optional[tuple[Callable[[], None], Callable[[], None]]] = None,
    readiness: Optional[Callable[[], bool]] = None,
) -> FastAPI:
    """FastAPI app with request metrics, structured access logs, /healthz, /readyz, /metrics."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if lifespan_hooks:
            lifespan_hooks[0]()
        log(logger, logging.INFO, "service started", event="startup")
        yield
        if lifespan_hooks:
            lifespan_hooks[1]()
        log(logger, logging.INFO, "service stopped", event="shutdown")

    app = FastAPI(title=title, lifespan=lifespan)

    @app.middleware("http")
    async def observe(request: Request, call_next):
        start = time.perf_counter()
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        HTTP_IN_FLIGHT.labels(SERVICE_NAME).inc()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-request-id"] = request_id
            response.headers["x-served-by"] = SERVICE_NAME
            return response
        finally:
            elapsed = time.perf_counter() - start
            HTTP_IN_FLIGHT.labels(SERVICE_NAME).dec()
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            if route_path not in _SKIP_METRICS_ROUTES:
                HTTP_REQUESTS.labels(SERVICE_NAME, request.method, route_path, str(status)).inc()
                HTTP_LATENCY.labels(SERVICE_NAME, request.method, route_path).observe(elapsed)
                level = logging.ERROR if status >= 500 else logging.INFO
                log(
                    logger, level, "request",
                    method=request.method, route=route_path, path=request.url.path,
                    status=status, duration_ms=round(elapsed * 1000, 2), request_id=request_id,
                )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        logger.exception("unhandled error", extra={"fields": {"path": request.url.path}})
        return JSONResponse(status_code=500, content={"detail": "internal error", "error": type(exc).__name__})

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"status": "ok", "service": SERVICE_NAME}

    @app.get("/readyz", include_in_schema=False)
    def readyz():
        if readiness is not None and not readiness():
            raise HTTPException(status_code=503, detail="not ready")
        return {"status": "ready", "service": SERVICE_NAME}

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


# --------------------------------------------------------------------------- Kafka
def kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")


def ensure_topics(topics: Iterable[str]) -> None:
    """Idempotently create Kafka topics (MSK also has auto.create enabled)."""
    bootstrap = kafka_bootstrap()
    if not bootstrap:
        log(logger, logging.WARNING, "KAFKA_BOOTSTRAP_SERVERS not set; skipping topic creation")
        return
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    partitions = env_int("KAFKA_PARTITIONS", 6)
    replication = env_int("KAFKA_REPLICATION_FACTOR", 2)
    new_topics = [NewTopic(t, num_partitions=partitions, replication_factor=replication) for t in topics]
    futures = admin.create_topics(new_topics, request_timeout=15)
    for topic, future in futures.items():
        try:
            future.result()
            log(logger, logging.INFO, "kafka topic created", topic=topic)
        except Exception as exc:  # TOPIC_ALREADY_EXISTS is expected on every restart
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc):
                continue
            log(logger, logging.WARNING, "kafka topic creation failed", topic=topic, error=str(exc))


class EventProducer:
    """Thread-safe, lazily connected, idempotent Kafka producer emitting JSON."""

    def __init__(self) -> None:
        self._producer = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._poller: Optional[threading.Thread] = None

    def _get(self):
        if self._producer is not None:
            return self._producer
        with self._lock:
            if self._producer is None:
                bootstrap = kafka_bootstrap()
                if not bootstrap:
                    return None
                from confluent_kafka import Producer

                self._producer = Producer({
                    "bootstrap.servers": bootstrap,
                    "client.id": SERVICE_NAME,
                    "enable.idempotence": True,
                    "acks": "all",
                    "linger.ms": 20,
                    "compression.type": "lz4",
                    "message.timeout.ms": 30000,
                })
                self._poller = threading.Thread(target=self._poll_loop, daemon=True)
                self._poller.start()
        return self._producer

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.5)

    @staticmethod
    def _delivery(topic: str):
        def callback(err, _msg):
            if err is not None:
                KAFKA_MESSAGES.labels(SERVICE_NAME, topic, "produce", "error").inc()
                log(logger, logging.WARNING, "kafka delivery failed", topic=topic, error=str(err))
            else:
                KAFKA_MESSAGES.labels(SERVICE_NAME, topic, "produce", "ok").inc()
        return callback

    def send(self, topic: str, key: str, value: dict[str, Any]) -> bool:
        producer = self._get()
        if producer is None:
            KAFKA_MESSAGES.labels(SERVICE_NAME, topic, "produce", "disabled").inc()
            return False
        try:
            producer.produce(
                topic,
                key=key.encode("utf-8"),
                value=json.dumps(value, default=str).encode("utf-8"),
                on_delivery=self._delivery(topic),
            )
            return True
        except BufferError:
            producer.poll(1)
            producer.produce(
                topic, key=key.encode("utf-8"),
                value=json.dumps(value, default=str).encode("utf-8"),
                on_delivery=self._delivery(topic),
            )
            return True
        except Exception as exc:
            KAFKA_MESSAGES.labels(SERVICE_NAME, topic, "produce", "error").inc()
            log(logger, logging.ERROR, "kafka produce failed", topic=topic, error=str(exc))
            return False

    def close(self) -> None:
        self._stop.set()
        if self._producer is not None:
            self._producer.flush(10)


def publish_invalidation(producer: EventProducer, keys: list[str], reason: str) -> None:
    topic = os.getenv("TOPIC_CACHE_INVALIDATION", "cache-invalidation")
    producer.send(topic, keys[0] if keys else "none", {
        "event_id": uuid.uuid4().hex,
        "keys": keys,
        "reason": reason,
        "source": SERVICE_NAME,
        "ts": now_ms(),
    })


# --------------------------------------------------------------------------- PostgreSQL
def make_pg_pool(schema: str, ddl: str):
    """Create a psycopg connection pool bound to `schema` and run idempotent DDL
    under an advisory lock (safe when many replicas start together)."""
    from psycopg_pool import ConnectionPool

    conninfo = (
        f"host={env('DB_HOST', required=True)} port={env('DB_PORT', '5432')} "
        f"dbname={env('DB_NAME', 's24')} user={env('DB_USER', required=True)} "
        f"password={env('DB_PASSWORD', required=True)} sslmode={env('DB_SSLMODE', 'require')} "
        f"connect_timeout=5 application_name={SERVICE_NAME}"
    )
    pool = ConnectionPool(
        conninfo,
        min_size=env_int("DB_POOL_MIN", 1),
        max_size=env_int("DB_POOL_MAX", 10),
        kwargs={"autocommit": True, "options": f"-c search_path={schema},public"},
        open=False,
    )
    last_error: Optional[Exception] = None
    for attempt in range(1, 31):
        try:
            pool.open(wait=True, timeout=10)
            with pool.connection() as conn:
                conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (f"schema:{schema}",))
                try:
                    conn.execute(ddl)
                finally:
                    conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"schema:{schema}",))
            log(logger, logging.INFO, "database ready", schema=schema)
            return pool
        except Exception as exc:
            last_error = exc
            log(logger, logging.WARNING, "database not ready, retrying", attempt=attempt, error=str(exc))
            time.sleep(min(2 * attempt, 10))
    raise RuntimeError(f"database unavailable: {last_error}")


# --------------------------------------------------------------------------- REST client
class ServiceClient:
    """Small wrapper around httpx with metrics, request-id propagation and
    upstream-error translation into HTTPException."""

    def __init__(self, base_url: str, name: str, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout, connect=2.0),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )

    def request(self, method: str, path: str, expected: tuple[int, ...] = (200, 201, 204), **kwargs) -> Any:
        try:
            resp = self.client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            DOWNSTREAM_CALLS.labels(SERVICE_NAME, self.name, "error").inc()
            raise HTTPException(status_code=503, detail=f"{self.name} unavailable: {type(exc).__name__}")
        DOWNSTREAM_CALLS.labels(SERVICE_NAME, self.name, str(resp.status_code)).inc()
        if resp.status_code not in expected:
            try:
                detail = resp.json().get("detail", resp.text)
            except ValueError:
                detail = resp.text
            status = resp.status_code if resp.status_code < 500 else 502
            raise HTTPException(status_code=status, detail=detail)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    def get(self, path: str, **kwargs) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> Any:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs) -> Any:
        return self.request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs) -> Any:
        return self.request("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs) -> Any:
        return self.request("DELETE", path, **kwargs)
