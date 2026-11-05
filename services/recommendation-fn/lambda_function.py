"""Service 7 – Recommendation Service (AWS Lambda, serverless, event-driven, asynchronous).

Two asynchronous triggers (both provisioned by Terraform):

  1. S3 ObjectCreated under videos/  (async invoke, failures -> SQS DLQ)
     A newly uploaded lesson video is registered in `LessonPopularity` with a
     "new content" boost so fresh lessons surface in recommendations.

  2. Amazon MSK topic `usage-analytics-results`  (event-source mapping)
     Flink's 1-minute window aggregates are stored in `UsageAnalytics`
     (NoSQL time series) and folded into each lesson's popularity score.

After either trigger the function recomputes ranked recommendation lists
(GLOBAL and per course) into the `Recommendations` table, which the Content
Delivery API serves (filtering out lessons the learner already completed).

Score model (time-decayed popularity, half-life SCORE_HALF_LIFE_HOURS):
    increment = 1.0 * unique_users + 3.0 * completions + 0.2 * views
    rank      = score * 0.5 ** (age_hours / half_life) + boost * 0.5 ** (age_since_upload / 72h)
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POPULARITY_TABLE = os.environ.get("POPULARITY_TABLE", "LessonPopularity")
RECOMMENDATIONS_TABLE = os.environ.get("RECOMMENDATIONS_TABLE", "Recommendations")
ANALYTICS_TABLE = os.environ.get("ANALYTICS_TABLE", "UsageAnalytics")
VIDEO_PREFIX = os.environ.get("VIDEO_PREFIX", "videos/")
TOP_N = int(os.environ.get("TOP_N", "20"))
HALF_LIFE_HOURS = float(os.environ.get("SCORE_HALF_LIFE_HOURS", "24"))
NEW_CONTENT_BOOST = float(os.environ.get("NEW_CONTENT_BOOST", "5"))
ANALYTICS_TTL_DAYS = int(os.environ.get("ANALYTICS_TTL_DAYS", "7"))
BOOST_HALF_LIFE_HOURS = 72.0

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
popularity = dynamodb.Table(POPULARITY_TABLE)
recommendations = dynamodb.Table(RECOMMENDATIONS_TABLE)
analytics = dynamodb.Table(ANALYTICS_TABLE)


def _log(msg: str, **fields) -> None:
    logger.info(json.dumps({"service": "recommendation-fn", "msg": msg, **fields}, default=str))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dec(value) -> Decimal:
    return Decimal(str(round(float(value), 4)))


def _names(expression: str) -> dict:
    """#Placeholders for every attribute (several, e.g. Views, are DynamoDB reserved words)."""
    return {token: token[1:] for token in re.findall(r"#\w+", expression)}


# =============================================================================
# Trigger 1: S3 upload
# =============================================================================

def handle_s3(records: list[dict]) -> dict:
    registered = 0
    for record in records:
        if not record.get("eventName", "").startswith("ObjectCreated"):
            continue
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        if not key.startswith(VIDEO_PREFIX):
            continue
        parts = key.split("/")
        if len(parts) < 3:
            _log("unexpected key layout, skipping", key=key)
            continue
        lesson_id = parts[1]
        head = s3.head_object(Bucket=bucket, Key=key)
        meta = head.get("Metadata", {})
        course_id = meta.get("course-id", "")
        now = _now_iso()
        expression = (
            "SET #ObjectKey = :k, #SizeBytes = :s, #ContentType = :ct, #UploadedAt = :u, #Boost = :b, "
            "#Score = if_not_exists(#Score, :zero), #LastUpdated = if_not_exists(#LastUpdated, :u), "
            "#Views = if_not_exists(#Views, :zero), #Completions = if_not_exists(#Completions, :zero), "
            "#UniqueUsers = if_not_exists(#UniqueUsers, :zero)"
        )
        values = {
            ":k": key, ":s": int(head.get("ContentLength", 0)), ":ct": head.get("ContentType", ""),
            ":u": now, ":b": _dec(NEW_CONTENT_BOOST), ":zero": Decimal(0),
        }
        if course_id:
            expression += ", #CourseID = :c"
            values[":c"] = course_id
        if meta.get("title"):
            expression += ", #Title = :t"
            values[":t"] = meta["title"]
        popularity.update_item(Key={"LessonID": lesson_id}, UpdateExpression=expression,
                               ExpressionAttributeNames=_names(expression),
                               ExpressionAttributeValues=values)
        registered += 1
        _log("new lesson video registered", lesson_id=lesson_id, course_id=course_id, key=key,
             size=head.get("ContentLength"))
    return {"registered_uploads": registered}


# =============================================================================
# Trigger 2: Kafka (MSK) – Flink window aggregates
# =============================================================================

def _decode_kafka(event: dict) -> list[dict]:
    out = []
    for _partition, messages in event.get("records", {}).items():
        for msg in messages:
            raw = msg.get("value")
            if not raw:
                continue
            try:
                out.append(json.loads(base64.b64decode(raw)))
            except (ValueError, TypeError):
                _log("bad kafka payload skipped", topic=msg.get("topic"), offset=msg.get("offset"))
    return out


def handle_kafka(event: dict) -> dict:
    aggregates = _decode_kafka(event)
    expires = int(time.time()) + ANALYTICS_TTL_DAYS * 86400
    lessons_updated = 0
    with analytics.batch_writer(overwrite_by_pkeys=["ItemID", "WindowStart"]) as batch:
        for agg in aggregates:
            item_id = str(agg.get("item_id", ""))
            if not item_id:
                continue
            batch.put_item(Item={
                "ItemID": item_id,
                "WindowStart": str(agg["window_start"]),
                "WindowEnd": str(agg["window_end"]),
                "CourseID": agg.get("course_id") or "",
                "unique_users": int(agg.get("unique_users", 0)),
                "views": int(agg.get("views", 0)),
                "completions": int(agg.get("completions", 0)),
                "progress_events": int(agg.get("progress_events", 0)),
                "watch_seconds": _dec(agg.get("watch_seconds", 0)),
                "cache_hits": int(agg.get("cache_hits", 0)),
                "cache_misses": int(agg.get("cache_misses", 0)),
                "stale_reads": int(agg.get("stale_reads", 0)),
                "stale_probes": int(agg.get("stale_probes", 0)),
                "ExpiresAt": expires,
            })

    for agg in aggregates:
        item_id = str(agg.get("item_id", ""))
        if not item_id or ":" in item_id:  # course-level ("course:<id>") / list items are not lessons
            continue
        users, completions, views = int(agg.get("unique_users", 0)), int(agg.get("completions", 0)), int(agg.get("views", 0))
        increment = 1.0 * users + 3.0 * completions + 0.2 * views
        if increment <= 0:
            continue
        expression = ("SET #LastUpdated = :now, #Score = if_not_exists(#Score, :zero) + :inc "
                      "ADD #Views :v, #Completions :c, #UniqueUsers :u")
        values = {":now": _now_iso(), ":zero": Decimal(0), ":inc": _dec(increment),
                  ":v": views, ":c": completions, ":u": users}
        if agg.get("course_id"):
            expression = expression.replace("SET ", "SET #CourseID = if_not_exists(#CourseID, :cid), ", 1)
            values[":cid"] = agg["course_id"]
        popularity.update_item(Key={"LessonID": item_id}, UpdateExpression=expression,
                               ExpressionAttributeNames=_names(expression),
                               ExpressionAttributeValues=values)
        lessons_updated += 1
    _log("window aggregates processed", aggregates=len(aggregates), lessons_updated=lessons_updated)
    return {"aggregates": len(aggregates), "lessons_updated": lessons_updated}


# =============================================================================
# Ranking
# =============================================================================

def _hours_since(iso: str | None) -> float:
    if not iso:
        return 1e6
    try:
        then = datetime.fromisoformat(iso)
    except ValueError:
        return 1e6
    return max(0.0, (datetime.now(timezone.utc) - then).total_seconds() / 3600)


def rank_score(item: dict) -> float:
    score = float(item.get("Score", 0)) * 0.5 ** (_hours_since(item.get("LastUpdated")) / HALF_LIFE_HOURS)
    boost = float(item.get("Boost", 0)) * 0.5 ** (_hours_since(item.get("UploadedAt")) / BOOST_HALF_LIFE_HOURS)
    return round(score + boost, 4)


def recompute_recommendations() -> dict:
    items, kwargs = [], {}
    while True:
        page = popularity.scan(**kwargs)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    scored = sorted(
        ({
            "lesson_id": i["LessonID"],
            "course_id": i.get("CourseID", ""),
            "score": rank_score(i),
            "views": int(i.get("Views", 0)),
            "completions": int(i.get("Completions", 0)),
            "unique_users": int(i.get("UniqueUsers", 0)),
            "is_new": _hours_since(i.get("UploadedAt")) < 72,
        } for i in items),
        key=lambda x: x["score"], reverse=True,
    )

    def as_ddb(entries: list[dict]) -> list[dict]:
        return [{**e, "score": _dec(e["score"])} for e in entries]

    generated = _now_iso()
    scopes = {"GLOBAL": scored[:TOP_N]}
    for entry in scored:
        if entry["course_id"]:
            bucket = scopes.setdefault(f"COURSE#{entry['course_id']}", [])
            if len(bucket) < TOP_N:
                bucket.append(entry)

    with recommendations.batch_writer() as batch:
        for scope, entries in scopes.items():
            batch.put_item(Item={
                "Scope": scope, "Lessons": as_ddb(entries), "GeneratedAt": generated,
                "Algorithm": f"decayed-popularity(half_life={HALF_LIFE_HOURS}h)+new-content-boost",
            })
    _log("recommendations recomputed", lessons=len(scored), scopes=len(scopes))
    return {"lessons_ranked": len(scored), "scopes": len(scopes)}


# =============================================================================
# Entry point
# =============================================================================

def lambda_handler(event, context):
    if event.get("eventSource") in ("aws:kafka", "aws:SelfManagedKafka"):
        result = handle_kafka(event)
    elif event.get("Records") and event["Records"][0].get("eventSource") == "aws:s3":
        result = handle_s3(event["Records"])
    else:
        _log("unsupported event ignored", keys=list(event.keys()))
        return {"status": "ignored"}
    result.update(recompute_recommendations())
    return {"status": "ok", **result}
