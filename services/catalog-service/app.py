"""Service 2 – Course Catalog Service (REST, PostgreSQL on RDS).

Owns courses and their ordered lesson listings. Every mutation bumps a
`version` column and publishes a cache-invalidation event to Kafka so the
Cache Manager drops the affected keys (course:<id>, lesson:<id>, courses:list)."""

import os

os.environ.setdefault("SERVICE_NAME", "catalog-service")

import logging
import uuid
from typing import Optional

from fastapi import HTTPException, Query
from psycopg import errors as pg_errors
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from common.s24common import EventProducer, create_app, log, logger, make_pg_pool, publish_invalidation

DDL = """
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE TABLE IF NOT EXISTS catalog.courses (
    id             UUID PRIMARY KEY,
    title          TEXT NOT NULL,
    description    TEXT NOT NULL DEFAULT '',
    category       TEXT NOT NULL DEFAULT 'general',
    level          TEXT NOT NULL DEFAULT 'beginner',
    instructor_id  UUID NOT NULL,
    published      BOOLEAN NOT NULL DEFAULT TRUE,
    version        BIGINT NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS catalog.lessons (
    id                UUID PRIMARY KEY,
    course_id         UUID NOT NULL REFERENCES catalog.courses(id) ON DELETE CASCADE,
    title             TEXT NOT NULL,
    summary           TEXT NOT NULL DEFAULT '',
    position          INTEGER NOT NULL,
    duration_seconds  INTEGER NOT NULL DEFAULT 0,
    version           BIGINT NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (course_id, position)
);
CREATE INDEX IF NOT EXISTS lessons_course_idx ON catalog.lessons (course_id, position);
CREATE INDEX IF NOT EXISTS courses_category_idx ON catalog.courses (category);
"""

COURSE_COLS = (
    "id::text AS id, title, description, category, level, instructor_id::text AS instructor_id, "
    "published, version, created_at, updated_at"
)
LESSON_COLS = (
    "id::text AS id, course_id::text AS course_id, title, summary, position, duration_seconds, "
    "version, created_at, updated_at"
)


class State:
    pool = None
    producer = EventProducer()


def startup() -> None:
    State.pool = make_pg_pool("catalog", DDL)


def shutdown() -> None:
    State.producer.close()
    if State.pool is not None:
        State.pool.close()


app = create_app("S24 Course Catalog Service", (startup, shutdown), readiness=lambda: State.pool is not None)


class CourseCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=5000)
    category: str = Field(default="general", max_length=60)
    level: str = Field(default="beginner", pattern="^(beginner|intermediate|advanced)$")
    instructor_id: str


class CourseUpdate(BaseModel):
    title: Optional[str] = Field(default=None, min_length=1, max_length=200)
    description: Optional[str] = Field(default=None, max_length=5000)
    category: Optional[str] = Field(default=None, max_length=60)
    level: Optional[str] = Field(default=None, pattern="^(beginner|intermediate|advanced)$")
    published: Optional[bool] = None


class LessonCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    summary: str = Field(default="", max_length=2000)
    position: Optional[int] = Field(default=None, ge=1)
    duration_seconds: int = Field(default=0, ge=0)


class LessonUpdate(BaseModel):
    title: Optional[str] = Field(default=None, min_length=1, max_length=200)
    summary: Optional[str] = Field(default=None, max_length=2000)
    duration_seconds: Optional[int] = Field(default=None, ge=0)


def _uuid(value: str, what: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise HTTPException(status_code=404, detail=f"{what} not found")


def _course_with_lessons(conn, course_id: str) -> dict:
    cur = conn.cursor(row_factory=dict_row)
    course = cur.execute(f"SELECT {COURSE_COLS} FROM courses WHERE id = %s", (course_id,)).fetchone()
    if course is None:
        raise HTTPException(status_code=404, detail="course not found")
    course["lessons"] = cur.execute(
        f"SELECT {LESSON_COLS} FROM lessons WHERE course_id = %s ORDER BY position", (course_id,)
    ).fetchall()
    course["lesson_count"] = len(course["lessons"])
    course["total_duration_seconds"] = sum(l["duration_seconds"] for l in course["lessons"])
    return course


def _bump_course(conn, course_id: str) -> None:
    conn.execute("UPDATE courses SET version = version + 1, updated_at = now() WHERE id = %s", (course_id,))


# --------------------------------------------------------------------------- courses
@app.post("/courses", status_code=201)
def create_course(body: CourseCreate):
    course_id = str(uuid.uuid4())
    with State.pool.connection() as conn:
        conn.execute(
            "INSERT INTO courses (id, title, description, category, level, instructor_id) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (course_id, body.title, body.description, body.category, body.level,
             _uuid(body.instructor_id, "instructor")),
        )
        course = _course_with_lessons(conn, course_id)
    publish_invalidation(State.producer, ["courses:list"], "course-created")
    log(logger, logging.INFO, "course created", course_id=course_id)
    return course


@app.get("/courses")
def list_courses(
    category: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    params: list = []
    where = "WHERE c.published"
    if category:
        where += " AND c.category = %s"
        params.append(category)
    sql = (
        "SELECT c.id::text AS id, c.title, c.description, c.category, c.level, "
        "c.instructor_id::text AS instructor_id, c.version, c.updated_at, "
        "COUNT(l.id) AS lesson_count, COALESCE(SUM(l.duration_seconds), 0) AS total_duration_seconds "
        f"FROM courses c LEFT JOIN lessons l ON l.course_id = c.id {where} "
        "GROUP BY c.id ORDER BY c.created_at DESC LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])
    with State.pool.connection() as conn:
        rows = conn.cursor(row_factory=dict_row).execute(sql, params).fetchall()
        list_version = conn.execute(
            "SELECT COALESCE(SUM(version), 0) + COUNT(*) FROM courses"
        ).fetchone()[0]
    return {"items": rows, "limit": limit, "offset": offset, "version": int(list_version)}


@app.get("/courses/{course_id}")
def get_course(course_id: str):
    with State.pool.connection() as conn:
        return _course_with_lessons(conn, _uuid(course_id, "course"))


@app.put("/courses/{course_id}")
def update_course(course_id: str, body: CourseUpdate):
    cid = _uuid(course_id, "course")
    fields = body.model_dump(exclude_none=True)
    if not fields:
        raise HTTPException(status_code=400, detail="no fields to update")
    assignments = ", ".join(f"{k} = %s" for k in fields)
    with State.pool.connection() as conn:
        result = conn.execute(
            f"UPDATE courses SET {assignments}, version = version + 1, updated_at = now() WHERE id = %s",
            (*fields.values(), cid),
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="course not found")
        course = _course_with_lessons(conn, cid)
    publish_invalidation(State.producer, [f"course:{cid}", "courses:list"], "course-updated")
    return course


@app.delete("/courses/{course_id}", status_code=204)
def delete_course(course_id: str):
    cid = _uuid(course_id, "course")
    with State.pool.connection() as conn:
        lesson_ids = [r[0] for r in conn.execute(
            "SELECT id::text FROM lessons WHERE course_id = %s", (cid,)
        ).fetchall()]
        result = conn.execute("DELETE FROM courses WHERE id = %s", (cid,))
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="course not found")
    publish_invalidation(
        State.producer,
        [f"course:{cid}", "courses:list"] + [f"lesson:{lid}" for lid in lesson_ids],
        "course-deleted",
    )
    return None


# --------------------------------------------------------------------------- lessons
@app.post("/courses/{course_id}/lessons", status_code=201)
def create_lesson(course_id: str, body: LessonCreate):
    cid = _uuid(course_id, "course")
    lesson_id = str(uuid.uuid4())
    with State.pool.connection() as conn:
        with conn.transaction():
            exists = conn.execute("SELECT 1 FROM courses WHERE id = %s FOR UPDATE", (cid,)).fetchone()
            if exists is None:
                raise HTTPException(status_code=404, detail="course not found")
            position = body.position or conn.execute(
                "SELECT COALESCE(MAX(position), 0) + 1 FROM lessons WHERE course_id = %s", (cid,)
            ).fetchone()[0]
            try:
                conn.execute(
                    "INSERT INTO lessons (id, course_id, title, summary, position, duration_seconds) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (lesson_id, cid, body.title, body.summary, position, body.duration_seconds),
                )
            except pg_errors.UniqueViolation:
                raise HTTPException(status_code=409, detail=f"position {position} already used")
            _bump_course(conn, cid)
        lesson = conn.cursor(row_factory=dict_row).execute(
            f"SELECT {LESSON_COLS} FROM lessons WHERE id = %s", (lesson_id,)
        ).fetchone()
    publish_invalidation(State.producer, [f"course:{cid}", "courses:list"], "lesson-created")
    log(logger, logging.INFO, "lesson created", course_id=cid, lesson_id=lesson_id)
    return lesson


@app.get("/lessons/{lesson_id}")
def get_lesson(lesson_id: str):
    with State.pool.connection() as conn:
        lesson = conn.cursor(row_factory=dict_row).execute(
            f"SELECT {LESSON_COLS} FROM lessons WHERE id = %s", (_uuid(lesson_id, "lesson"),)
        ).fetchone()
    if lesson is None:
        raise HTTPException(status_code=404, detail="lesson not found")
    return lesson


@app.put("/lessons/{lesson_id}")
def update_lesson(lesson_id: str, body: LessonUpdate):
    lid = _uuid(lesson_id, "lesson")
    fields = body.model_dump(exclude_none=True)
    if not fields:
        raise HTTPException(status_code=400, detail="no fields to update")
    assignments = ", ".join(f"{k} = %s" for k in fields)
    with State.pool.connection() as conn:
        with conn.transaction():
            row = conn.execute(
                f"UPDATE lessons SET {assignments}, version = version + 1, updated_at = now() "
                "WHERE id = %s RETURNING course_id::text",
                (*fields.values(), lid),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="lesson not found")
            _bump_course(conn, row[0])
        lesson = conn.cursor(row_factory=dict_row).execute(
            f"SELECT {LESSON_COLS} FROM lessons WHERE id = %s", (lid,)
        ).fetchone()
    publish_invalidation(
        State.producer, [f"lesson:{lid}", f"course:{lesson['course_id']}", "courses:list"], "lesson-updated"
    )
    return lesson


@app.delete("/lessons/{lesson_id}", status_code=204)
def delete_lesson(lesson_id: str):
    lid = _uuid(lesson_id, "lesson")
    with State.pool.connection() as conn:
        with conn.transaction():
            row = conn.execute("DELETE FROM lessons WHERE id = %s RETURNING course_id::text", (lid,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="lesson not found")
            _bump_course(conn, row[0])
    publish_invalidation(State.producer, [f"lesson:{lid}", f"course:{row[0]}", "courses:list"], "lesson-deleted")
    return None
