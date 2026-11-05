"""Service 6 – Progress Tracker (REST, DynamoDB + Kafka producer).

Ingests high-frequency learner playback events (play / pause / seek /
heartbeat / complete). Each event
  1. upserts the learner's state for the lesson in DynamoDB `LearnerProgress`
     (PK UserID, SK LessonID) with atomic counters, and
  2. is published to the Kafka topic `learner-progress-events` (keyed by
     lesson) for the Flink usage-analytics job."""

import os

os.environ.setdefault("SERVICE_NAME", "progress-tracker")

import logging
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal, Optional

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import HTTPException, Query
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

from common.s24common import EventProducer, create_app, ensure_topics, env, env_float, log, logger, now_ms

TABLE = env("PROGRESS_TABLE", "LearnerProgress")
TOPIC = env("TOPIC_PROGRESS_EVENTS", "learner-progress-events")
COMPLETION_THRESHOLD = env_float("COMPLETION_THRESHOLD_PERCENT", 90.0)

EVENTS = Counter("progress_events_total", "Progress events ingested", ["event", "result"])
COMPLETIONS = Counter("progress_completions_total", "Lessons newly completed")
DDB_LATENCY = Histogram("progress_dynamodb_seconds", "DynamoDB write latency",
                        buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0))


class State:
    ddb = None
    producer = EventProducer()
    ser = TypeSerializer()
    deser = TypeDeserializer()


def startup() -> None:
    State.ddb = boto3.client(
        "dynamodb", region_name=env("AWS_REGION", "ap-south-1"),
        config=Config(retries={"max_attempts": 5, "mode": "adaptive"}, max_pool_connections=50),
    )
    ensure_topics([TOPIC])


def shutdown() -> None:
    State.producer.close()


app = create_app("S24 Progress Tracker", (startup, shutdown))


class ProgressEvent(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    lesson_id: str = Field(min_length=1, max_length=64)
    course_id: Optional[str] = Field(default=None, max_length=64)
    event: Literal["play", "pause", "seek", "heartbeat", "complete"] = "heartbeat"
    position_seconds: float = Field(ge=0)
    duration_seconds: float = Field(gt=0)
    watched_delta_seconds: float = Field(default=0, ge=0, le=3600)
    client_ts: Optional[int] = Field(default=None, description="epoch milliseconds on the player")


def _num(value: float) -> Decimal:
    return Decimal(str(round(value, 3)))


def _plain(item: dict) -> dict:
    out = {}
    for k, v in item.items():
        val = State.deser.deserialize(v)
        if isinstance(val, Decimal):
            val = int(val) if val == val.to_integral_value() else float(val)
        out[k] = val
    return {
        "user_id": out.get("UserID"),
        "lesson_id": out.get("LessonID"),
        "course_id": out.get("CourseID"),
        "position_seconds": out.get("PositionSeconds"),
        "duration_seconds": out.get("DurationSeconds"),
        "percent_complete": out.get("PercentComplete"),
        "completed": out.get("Completed", False),
        "completed_at": out.get("CompletedAt"),
        "watch_seconds": out.get("WatchSeconds", 0),
        "event_count": out.get("EventCount", 0),
        "last_event": out.get("LastEvent"),
        "first_seen_at": out.get("FirstSeenAt"),
        "updated_at": out.get("UpdatedAt"),
    }


def _record(evt: ProgressEvent) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    position = min(evt.position_seconds, evt.duration_seconds)
    percent = min(100.0, position / evt.duration_seconds * 100.0)
    completed_now = evt.event == "complete" or percent >= COMPLETION_THRESHOLD

    sets = [
        "PositionSeconds = :pos", "DurationSeconds = :dur", "PercentComplete = :pct",
        "LastEvent = :evt", "UpdatedAt = :now", "FirstSeenAt = if_not_exists(FirstSeenAt, :now)",
    ]
    values = {
        ":pos": _num(position), ":dur": _num(evt.duration_seconds), ":pct": _num(percent),
        ":evt": evt.event, ":now": now_iso, ":w": _num(evt.watched_delta_seconds), ":one": 1,
    }
    if evt.course_id:
        sets.append("CourseID = :cid")
        values[":cid"] = evt.course_id
    if completed_now:
        sets += ["Completed = :t", "CompletedAt = if_not_exists(CompletedAt, :now)"]
        values[":t"] = True
    else:
        sets.append("Completed = if_not_exists(Completed, :f)")
        values[":f"] = False

    t0 = time.perf_counter()
    resp = State.ddb.update_item(
        TableName=TABLE,
        Key={"UserID": {"S": evt.user_id}, "LessonID": {"S": evt.lesson_id}},
        UpdateExpression="SET " + ", ".join(sets) + " ADD WatchSeconds :w, EventCount :one",
        ExpressionAttributeValues={k: State.ser.serialize(v) for k, v in values.items()},
        ReturnValues="ALL_NEW",
    )
    DDB_LATENCY.observe(time.perf_counter() - t0)
    state = _plain(resp["Attributes"])
    newly_completed = completed_now and state["completed_at"] == now_iso
    if newly_completed:
        COMPLETIONS.inc()

    State.producer.send(TOPIC, evt.lesson_id, {
        "event_id": uuid.uuid4().hex,
        "user_id": evt.user_id,
        "lesson_id": evt.lesson_id,
        "course_id": evt.course_id or state.get("course_id") or "",
        "event": evt.event,
        "position_seconds": round(position, 3),
        "duration_seconds": evt.duration_seconds,
        "percent": round(percent, 2),
        "watched_delta_seconds": evt.watched_delta_seconds,
        "completed": bool(state["completed"]),
        "newly_completed": newly_completed,
        "event_time": evt.client_ts if evt.client_ts else now_ms(),
        "ingest_time": now_ms(),
    })
    EVENTS.labels(evt.event, "ok").inc()
    return state


@app.post("/progress", status_code=202)
def record_progress(evt: ProgressEvent):
    try:
        return _record(evt)
    except ClientError as exc:
        EVENTS.labels(evt.event, "error").inc()
        log(logger, logging.ERROR, "dynamodb write failed", error=str(exc), user_id=evt.user_id)
        raise HTTPException(status_code=503, detail="progress store unavailable")


@app.post("/progress/batch", status_code=202)
def record_batch(events: list[ProgressEvent]):
    if not 1 <= len(events) <= 100:
        raise HTTPException(status_code=400, detail="batch must contain 1..100 events")
    results, failed = [], 0
    for evt in sorted(events, key=lambda e: e.client_ts or 0):
        try:
            results.append(_record(evt))
        except ClientError:
            EVENTS.labels(evt.event, "error").inc()
            failed += 1
    return {"accepted": len(results), "failed": failed, "items": results}


@app.get("/progress/{user_id}")
def user_progress(user_id: str, completed: Optional[bool] = None, limit: int = Query(default=200, ge=1, le=1000)):
    kwargs = {
        "TableName": TABLE,
        "KeyConditionExpression": "UserID = :u",
        "ExpressionAttributeValues": {":u": {"S": user_id}},
        "Limit": limit,
    }
    if completed is not None:
        kwargs["FilterExpression"] = "Completed = :c"
        kwargs["ExpressionAttributeValues"][":c"] = {"BOOL": completed}
    items, start_key = [], None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = State.ddb.query(**kwargs)
        items.extend(_plain(i) for i in resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key or len(items) >= limit:
            break
    completed_count = sum(1 for i in items if i["completed"])
    return {
        "user_id": user_id, "items": items[:limit], "lessons_started": len(items),
        "lessons_completed": completed_count,
        "total_watch_seconds": round(sum(i["watch_seconds"] or 0 for i in items), 1),
    }


@app.get("/progress/{user_id}/{lesson_id}")
def lesson_progress(user_id: str, lesson_id: str):
    resp = State.ddb.get_item(
        TableName=TABLE, Key={"UserID": {"S": user_id}, "LessonID": {"S": lesson_id}}, ConsistentRead=True
    )
    if "Item" not in resp:
        raise HTTPException(status_code=404, detail="no progress for this lesson")
    return _plain(resp["Item"])


@app.get("/lessons/{lesson_id}/learners")
def lesson_learners(lesson_id: str, limit: int = Query(default=100, ge=1, le=1000)):
    resp = State.ddb.query(
        TableName=TABLE, IndexName="LessonIndex",
        KeyConditionExpression="LessonID = :l", ExpressionAttributeValues={":l": {"S": lesson_id}}, Limit=limit,
    )
    items = [_plain(i) for i in resp.get("Items", [])]
    return {
        "lesson_id": lesson_id, "learners": len(items),
        "completed": sum(1 for i in items if i["completed"]),
        "avg_percent": round(sum(i["percent_complete"] or 0 for i in items) / len(items), 2) if items else 0,
        "items": items,
    }


@app.delete("/progress/{user_id}/{lesson_id}", status_code=204)
def reset_progress(user_id: str, lesson_id: str):
    State.ddb.delete_item(TableName=TABLE, Key={"UserID": {"S": user_id}, "LessonID": {"S": lesson_id}})
    return None
