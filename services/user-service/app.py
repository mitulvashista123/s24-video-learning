"""Service 1 – User Service (REST, PostgreSQL on RDS).

Manages learner profiles and access roles (learner / instructor / admin) and
answers permission checks for the public Content Delivery API."""

import os

os.environ.setdefault("SERVICE_NAME", "user-service")

import logging
import uuid
from typing import Literal, Optional

from fastapi import HTTPException, Query
from psycopg import errors as pg_errors
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from common.s24common import create_app, log, logger, make_pg_pool

Role = Literal["learner", "instructor", "admin"]

PERMISSIONS: dict[str, list[str]] = {
    "learner": ["content:read", "progress:write", "progress:read:self", "recommendations:read"],
    "instructor": [
        "content:read", "progress:write", "progress:read:self", "recommendations:read",
        "course:write", "video:upload", "analytics:read",
    ],
    "admin": [
        "content:read", "progress:write", "progress:read:any", "recommendations:read",
        "course:write", "video:upload", "analytics:read", "user:admin", "cache:admin",
    ],
}

DDL = """
CREATE SCHEMA IF NOT EXISTS users;
CREATE TABLE IF NOT EXISTS users.learners (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    role        TEXT NOT NULL DEFAULT 'learner' CHECK (role IN ('learner', 'instructor', 'admin')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS learners_role_idx ON users.learners (role);
"""

COLUMNS = "id::text AS id, name, email, role, created_at, updated_at"


class State:
    pool = None


def startup() -> None:
    State.pool = make_pg_pool("users", DDL)


def shutdown() -> None:
    if State.pool is not None:
        State.pool.close()


app = create_app("S24 User Service", (startup, shutdown), readiness=lambda: State.pool is not None)


class UserCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=254)
    role: Role = "learner"


class RoleUpdate(BaseModel):
    role: Role


class UserUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    email: Optional[str] = Field(default=None, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=254)


def _valid_uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise HTTPException(status_code=404, detail="user not found")


def _fetch_user(user_id: str) -> dict:
    with State.pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(
            f"SELECT {COLUMNS} FROM learners WHERE id = %s", (_valid_uuid(user_id),)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="user not found")
    return row


@app.post("/users", status_code=201)
def create_user(body: UserCreate):
    user_id = str(uuid.uuid4())
    try:
        with State.pool.connection() as conn:
            row = conn.cursor(row_factory=dict_row).execute(
                f"INSERT INTO learners (id, name, email, role) VALUES (%s, %s, %s, %s) RETURNING {COLUMNS}",
                (user_id, body.name, body.email.lower(), body.role),
            ).fetchone()
    except pg_errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="email already registered")
    log(logger, logging.INFO, "user created", user_id=user_id, role=body.role)
    return row


@app.get("/users")
def list_users(
    role: Optional[Role] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    with State.pool.connection() as conn:
        cur = conn.cursor(row_factory=dict_row)
        if role:
            rows = cur.execute(
                f"SELECT {COLUMNS} FROM learners WHERE role = %s ORDER BY created_at LIMIT %s OFFSET %s",
                (role, limit, offset),
            ).fetchall()
        else:
            rows = cur.execute(
                f"SELECT {COLUMNS} FROM learners ORDER BY created_at LIMIT %s OFFSET %s", (limit, offset)
            ).fetchall()
    return {"items": rows, "limit": limit, "offset": offset}


@app.get("/users/{user_id}")
def get_user(user_id: str):
    return _fetch_user(user_id)


@app.patch("/users/{user_id}")
def update_user(user_id: str, body: UserUpdate):
    current = _fetch_user(user_id)
    name = body.name if body.name is not None else current["name"]
    email = body.email.lower() if body.email is not None else current["email"]
    try:
        with State.pool.connection() as conn:
            row = conn.cursor(row_factory=dict_row).execute(
                f"UPDATE learners SET name = %s, email = %s, updated_at = now() WHERE id = %s RETURNING {COLUMNS}",
                (name, email, current["id"]),
            ).fetchone()
    except pg_errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="email already registered")
    return row


@app.put("/users/{user_id}/role")
def update_role(user_id: str, body: RoleUpdate):
    _fetch_user(user_id)
    with State.pool.connection() as conn:
        row = conn.cursor(row_factory=dict_row).execute(
            f"UPDATE learners SET role = %s, updated_at = now() WHERE id = %s RETURNING {COLUMNS}",
            (body.role, _valid_uuid(user_id)),
        ).fetchone()
    log(logger, logging.INFO, "role changed", user_id=user_id, role=body.role)
    return row


@app.get("/users/{user_id}/permissions")
def permissions(user_id: str):
    user = _fetch_user(user_id)
    return {"user_id": user["id"], "role": user["role"], "permissions": PERMISSIONS[user["role"]]}


@app.delete("/users/{user_id}", status_code=204)
def delete_user(user_id: str):
    with State.pool.connection() as conn:
        result = conn.execute("DELETE FROM learners WHERE id = %s", (_valid_uuid(user_id),))
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="user not found")
    return None
