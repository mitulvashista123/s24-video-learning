"""Service 3 – Content Metadata Service (REST, PostgreSQL + S3).

Stores per-lesson video metadata (title, language, resolutions, subtitles,
status) in PostgreSQL and the binary assets (videos, WebVTT subtitles) in S3.
An upload under s3://<bucket>/videos/ triggers the serverless Recommendation
function. Every change publishes a cache-invalidation event."""

import os

os.environ.setdefault("SERVICE_NAME", "metadata-service")

import logging
import re
import uuid
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import File, HTTPException, Query, UploadFile
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from common.s24common import (
    EventProducer, ServiceClient, create_app, env, env_int, log, logger, make_pg_pool, publish_invalidation,
)

DDL = """
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE TABLE IF NOT EXISTS metadata.videos (
    lesson_id         UUID PRIMARY KEY,
    course_id         UUID,
    title             TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    language          TEXT NOT NULL DEFAULT 'en',
    duration_seconds  INTEGER NOT NULL DEFAULT 0,
    resolutions       JSONB NOT NULL DEFAULT '["720p"]'::jsonb,
    tags              JSONB NOT NULL DEFAULT '[]'::jsonb,
    subtitles         JSONB NOT NULL DEFAULT '[]'::jsonb,
    video_key         TEXT,
    video_size_bytes  BIGINT,
    content_type      TEXT,
    status            TEXT NOT NULL DEFAULT 'pending_upload'
                      CHECK (status IN ('pending_upload', 'uploading', 'uploaded')),
    version           BIGINT NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS videos_course_idx ON metadata.videos (course_id);
"""

COLS = (
    "lesson_id::text AS lesson_id, course_id::text AS course_id, title, description, language, "
    "duration_seconds, resolutions, tags, subtitles, video_key, video_size_bytes, content_type, status, "
    "version, created_at, updated_at"
)

ALLOWED_VIDEO_TYPES = {"video/mp4", "video/webm", "video/quicktime", "application/octet-stream"}
PRESIGN_SECONDS = env_int("PRESIGN_EXPIRY_SECONDS", 3600)
MAX_UPLOAD_BYTES = env_int("MAX_UPLOAD_BYTES", 512 * 1024 * 1024)


class State:
    pool = None
    s3 = None
    bucket = ""
    producer = EventProducer()
    catalog: Optional[ServiceClient] = None


def startup() -> None:
    region = env("AWS_REGION", "ap-south-1")
    State.bucket = env("S3_BUCKET", required=True)
    State.s3 = boto3.client(
        "s3", region_name=region,
        endpoint_url=env("AWS_ENDPOINT_URL_S3", "") or f"https://s3.{region}.amazonaws.com",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "adaptive"}),
    )
    State.catalog = ServiceClient(env("CATALOG_SERVICE_URL", "http://catalog-service"), "catalog-service")
    State.pool = make_pg_pool("metadata", DDL)


def shutdown() -> None:
    State.producer.close()
    if State.pool is not None:
        State.pool.close()


app = create_app("S24 Content Metadata Service", (startup, shutdown), readiness=lambda: State.pool is not None)


class VideoMetadata(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=5000)
    language: str = Field(default="en", pattern=r"^[a-z]{2}(-[A-Z]{2})?$")
    duration_seconds: int = Field(default=0, ge=0)
    resolutions: list[str] = Field(default_factory=lambda: ["720p"])
    tags: list[str] = Field(default_factory=list)


class UploadUrlRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=200)
    content_type: str = "video/mp4"


def _uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise HTTPException(status_code=404, detail="lesson not found")


def _safe_name(filename: str) -> str:
    base = os.path.basename(filename)
    return re.sub(r"[^A-Za-z0-9._-]", "_", base)[:120] or "upload.bin"


def _get_video(lesson_id: str) -> dict:
    with State.pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(
            f"SELECT {COLS} FROM videos WHERE lesson_id = %s", (lesson_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="video metadata not found")
    return row


def _invalidate(lesson_id: str, reason: str) -> None:
    publish_invalidation(State.producer, [f"video:{lesson_id}", f"stream:{lesson_id}"], reason)


@app.put("/videos/{lesson_id}")
def upsert_metadata(lesson_id: str, body: VideoMetadata):
    lid = _uuid(lesson_id)
    lesson = State.catalog.get(f"/lessons/{lid}")  # REST call: the lesson must exist in the catalog
    with State.pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(
            f"""
            INSERT INTO videos (lesson_id, course_id, title, description, language, duration_seconds,
                                resolutions, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (lesson_id) DO UPDATE SET
                title = EXCLUDED.title, description = EXCLUDED.description,
                language = EXCLUDED.language, duration_seconds = EXCLUDED.duration_seconds,
                resolutions = EXCLUDED.resolutions, tags = EXCLUDED.tags,
                version = videos.version + 1, updated_at = now()
            RETURNING {COLS}
            """,
            (lid, lesson["course_id"], body.title, body.description, body.language, body.duration_seconds,
             Jsonb(body.resolutions), Jsonb(body.tags)),
        ).fetchone()
    _invalidate(lid, "video-metadata-updated")
    return row


@app.get("/videos/{lesson_id}")
def get_metadata(lesson_id: str):
    return _get_video(_uuid(lesson_id))


@app.get("/videos")
def list_videos(course_id: Optional[str] = None, limit: int = Query(default=50, ge=1, le=200)):
    with State.pool.connection() as conn:
        cur = conn.cursor(row_factory=dict_row)
        if course_id:
            rows = cur.execute(
                f"SELECT {COLS} FROM videos WHERE course_id = %s ORDER BY created_at LIMIT %s",
                (_uuid(course_id), limit),
            ).fetchall()
        else:
            rows = cur.execute(f"SELECT {COLS} FROM videos ORDER BY created_at DESC LIMIT %s", (limit,)).fetchall()
    return {"items": rows}


@app.post("/videos/{lesson_id}/upload")
def upload_video(lesson_id: str, file: UploadFile = File(...)):
    """Direct multipart upload through the service (small/medium files)."""
    lid = _uuid(lesson_id)
    video = _get_video(lid)
    content_type = file.content_type or "application/octet-stream"
    if content_type not in ALLOWED_VIDEO_TYPES:
        raise HTTPException(status_code=415, detail=f"unsupported content type {content_type}")
    file.file.seek(0, os.SEEK_END)
    size = file.file.tell()
    file.file.seek(0)
    if size == 0 or size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"file size must be 1..{MAX_UPLOAD_BYTES} bytes")
    key = f"videos/{lid}/{uuid.uuid4().hex[:8]}-{_safe_name(file.filename or 'video.mp4')}"
    State.s3.upload_fileobj(
        file.file, State.bucket, key,
        ExtraArgs={
            "ContentType": content_type,
            "Metadata": {
                "lesson-id": lid,
                "course-id": video["course_id"] or "",
                "title": video["title"][:200].encode("ascii", "ignore").decode(),
            },
        },
    )
    with State.pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(
            f"UPDATE videos SET video_key = %s, video_size_bytes = %s, content_type = %s, status = 'uploaded', "
            f"version = version + 1, updated_at = now() WHERE lesson_id = %s RETURNING {COLS}",
            (key, size, content_type, lid),
        ).fetchone()
    _invalidate(lid, "video-uploaded")
    log(logger, logging.INFO, "video uploaded", lesson_id=lid, key=key, size=size)
    return row


@app.post("/videos/{lesson_id}/upload-url")
def presigned_upload(lesson_id: str, body: UploadUrlRequest):
    """Presigned PUT URL for large files uploaded straight from the browser to S3."""
    lid = _uuid(lesson_id)
    video = _get_video(lid)
    if body.content_type not in ALLOWED_VIDEO_TYPES:
        raise HTTPException(status_code=415, detail=f"unsupported content type {body.content_type}")
    key = f"videos/{lid}/{uuid.uuid4().hex[:8]}-{_safe_name(body.filename)}"
    url = State.s3.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": State.bucket, "Key": key, "ContentType": body.content_type,
            "Metadata": {"lesson-id": lid, "course-id": video["course_id"] or ""},
        },
        ExpiresIn=PRESIGN_SECONDS,
    )
    with State.pool.connection() as conn:
        conn.execute(
            "UPDATE videos SET video_key = %s, content_type = %s, status = 'uploading', "
            "version = version + 1, updated_at = now() WHERE lesson_id = %s",
            (key, body.content_type, lid),
        )
    _invalidate(lid, "video-upload-started")
    return {
        "upload_url": url, "method": "PUT", "key": key, "expires_in": PRESIGN_SECONDS,
        "required_headers": {
            "Content-Type": body.content_type,
            "x-amz-meta-lesson-id": lid,
            "x-amz-meta-course-id": video["course_id"] or "",
        },
    }


@app.post("/videos/{lesson_id}/upload-complete")
def upload_complete(lesson_id: str):
    """Called after a presigned upload finishes: verifies the object in S3."""
    lid = _uuid(lesson_id)
    video = _get_video(lid)
    if not video["video_key"]:
        raise HTTPException(status_code=409, detail="no upload in progress")
    try:
        head = State.s3.head_object(Bucket=State.bucket, Key=video["video_key"])
    except ClientError:
        raise HTTPException(status_code=409, detail="object not found in S3 yet")
    with State.pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(
            f"UPDATE videos SET video_size_bytes = %s, status = 'uploaded', version = version + 1, "
            f"updated_at = now() WHERE lesson_id = %s RETURNING {COLS}",
            (head["ContentLength"], lid),
        ).fetchone()
    _invalidate(lid, "video-upload-complete")
    return row


@app.post("/videos/{lesson_id}/subtitles")
def upload_subtitles(
    lesson_id: str,
    language: str = Query(pattern=r"^[a-z]{2}(-[A-Z]{2})?$"),
    file: UploadFile = File(...),
):
    lid = _uuid(lesson_id)
    _get_video(lid)
    data = file.file.read()
    if not data.lstrip(b"\xef\xbb\xbf").startswith(b"WEBVTT"):
        raise HTTPException(status_code=415, detail="subtitles must be WebVTT (.vtt)")
    key = f"subtitles/{lid}/{language}.vtt"
    State.s3.put_object(Bucket=State.bucket, Key=key, Body=data, ContentType="text/vtt")
    with State.pool.connection() as conn:
        with conn.transaction():
            current = conn.execute(
                "SELECT subtitles FROM videos WHERE lesson_id = %s FOR UPDATE", (lid,)
            ).fetchone()[0]
            subtitles = [s for s in current if s.get("language") != language]
            subtitles.append({"language": language, "key": key, "size_bytes": len(data)})
            row = conn.cursor(row_factory=dict_row).execute(
                f"UPDATE videos SET subtitles = %s, version = version + 1, updated_at = now() "
                f"WHERE lesson_id = %s RETURNING {COLS}",
                (Jsonb(subtitles), lid),
            ).fetchone()
    _invalidate(lid, "subtitles-updated")
    return row


@app.get("/videos/{lesson_id}/stream-url")
def stream_url(lesson_id: str):
    lid = _uuid(lesson_id)
    video = _get_video(lid)
    if video["status"] != "uploaded" or not video["video_key"]:
        raise HTTPException(status_code=409, detail="video not uploaded yet")
    video_url = State.s3.generate_presigned_url(
        "get_object", Params={"Bucket": State.bucket, "Key": video["video_key"]}, ExpiresIn=PRESIGN_SECONDS
    )
    subtitles = [
        {
            "language": s["language"],
            "url": State.s3.generate_presigned_url(
                "get_object", Params={"Bucket": State.bucket, "Key": s["key"]}, ExpiresIn=PRESIGN_SECONDS
            ),
        }
        for s in video["subtitles"]
    ]
    return {
        "lesson_id": lid, "video_url": video_url, "subtitles": subtitles,
        "expires_in": PRESIGN_SECONDS, "version": video["version"],
    }


@app.delete("/videos/{lesson_id}", status_code=204)
def delete_video(lesson_id: str):
    lid = _uuid(lesson_id)
    video = _get_video(lid)
    keys = [s["key"] for s in video["subtitles"]]
    if video["video_key"]:
        keys.append(video["video_key"])
    if keys:
        State.s3.delete_objects(Bucket=State.bucket, Delete={"Objects": [{"Key": k} for k in keys]})
    with State.pool.connection() as conn:
        conn.execute("DELETE FROM videos WHERE lesson_id = %s", (lid,))
    _invalidate(lid, "video-deleted")
    return None
