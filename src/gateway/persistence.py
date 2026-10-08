"""Durable state in Postgres.

What is stored: batches, requests (accepted work and its final outcome) and the current model limits.
What is not: per-attempt transitions (queued/in_flight changes) and rejected requests (the client was told 429).

Write path:
  * A batch and all its requests are inserted *before* the 202 acknowledgement, so an acknowledged batch survives a crash.
  * Everything else is write-behind: rows are collected (latest state per id wins) and flushed every `flush_interval_s`
    in one statement per table. A crash can lose up to one interval of final states; recovery then re-runs those
    requests (delivery to the provider is at-least-once).
  * If the database is unreachable, rows stay queued in memory and are retried; the gateway keeps serving.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

log = logging.getLogger("gateway.persistence")

OPEN_STATES = ("queued", "in_flight")
OPEN_CALLBACK = ("pending", "delivering", "retrying")

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id text PRIMARY KEY,
    owner text NOT NULL,
    callback_url text,
    status text NOT NULL,
    total int NOT NULL,
    succeeded int NOT NULL DEFAULT 0,
    failed int NOT NULL DEFAULT 0,
    expired int NOT NULL DEFAULT 0,
    created_at double precision NOT NULL,
    completed_at double precision,
    cb_status text NOT NULL DEFAULT 'none',
    cb_attempts int NOT NULL DEFAULT 0,
    cb_last_error text,
    cb_delivered_at double precision
);
CREATE TABLE IF NOT EXISTS requests (
    id text PRIMARY KEY,
    owner text NOT NULL,
    batch_id text,
    idx int NOT NULL DEFAULT 0,
    model text NOT NULL,
    tokens int NOT NULL,
    payload jsonb,
    state text NOT NULL,
    attempts int NOT NULL DEFAULT 0,
    error text,
    accepted_at double precision NOT NULL,
    finished_at double precision
);
CREATE INDEX IF NOT EXISTS requests_batch_idx ON requests (batch_id, idx);
CREATE INDEX IF NOT EXISTS requests_open_owner ON requests (owner) WHERE state IN ('queued', 'in_flight');
CREATE INDEX IF NOT EXISTS batches_open_owner ON batches (owner) WHERE status = 'processing' OR cb_status IN ('pending', 'delivering', 'retrying');
CREATE TABLE IF NOT EXISTS model_limits (
    model text PRIMARY KEY,
    rpm bigint NOT NULL,
    tpm bigint NOT NULL,
    updated_at double precision NOT NULL
);
"""

REQ_COLS = ("id", "owner", "batch_id", "idx", "model", "tokens", "payload", "state", "attempts", "error", "accepted_at", "finished_at")
REQ_TYPES = ("text", "text", "text", "int", "text", "int", "text", "text", "int", "text", "float8", "float8")
BATCH_COLS = ("id", "owner", "callback_url", "status", "total", "succeeded", "failed", "expired", "created_at", "completed_at",
              "cb_status", "cb_attempts", "cb_last_error", "cb_delivered_at")
BATCH_TYPES = ("text", "text", "text", "text", "int", "int", "int", "int", "float8", "float8", "text", "int", "text", "float8")


def _unnest_sql(table: str, cols, types, update_cols) -> str:
    args = ", ".join(f"${i + 1}::{t}[]" for i, t in enumerate(types))
    names = ", ".join(f"u{i}" for i in range(len(cols)))
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    return (f"INSERT INTO {table} ({', '.join(cols)}) "
            f"SELECT {', '.join(('u%d::jsonb' % i) if c == 'payload' else 'u%d' % i for i, c in enumerate(cols))} "
            f"FROM unnest({args}) AS t({names}) ON CONFLICT (id) DO UPDATE SET {sets}")


UPSERT_REQUESTS = _unnest_sql("requests", REQ_COLS, REQ_TYPES, ("owner", "state", "attempts", "error", "finished_at"))
UPSERT_BATCHES = _unnest_sql("batches", BATCH_COLS, BATCH_TYPES,
                             ("owner", "status", "succeeded", "failed", "expired", "completed_at",
                              "cb_status", "cb_attempts", "cb_last_error", "cb_delivered_at"))
INSERT_REQUESTS_NEW = UPSERT_REQUESTS.replace("DO UPDATE SET owner = EXCLUDED.owner, state = EXCLUDED.state, attempts = EXCLUDED.attempts, "
                                              "error = EXCLUDED.error, finished_at = EXCLUDED.finished_at", "DO NOTHING")


class NullPersistence:
    """No database: everything lives in memory."""
    enabled = False

    async def start(self): ...
    async def stop(self): ...
    def put_request(self, row): ...
    def put_batch(self, row): ...
    async def insert_batch(self, batch_row, req_rows): ...
    async def get_request(self, rid): return None
    async def get_batch(self, bid): return None
    async def batch_results(self, bid, offset, limit): return None
    async def limits(self): return {}
    async def save_limits(self, model, rpm, tpm): ...
    async def owners_with_open_work(self): return []
    async def claim(self, me, dead): return [], []
    async def truncate(self): ...


class Persistence:
    enabled = True

    def __init__(self, dsn: str, flush_interval_s: float = 0.1, max_flush_rows: int = 5000):
        self.dsn, self.flush_interval_s, self.max_flush_rows = dsn, flush_interval_s, max_flush_rows
        self._pool = None
        self._req: dict[str, tuple] = {}
        self._batch: dict[str, tuple] = {}
        self._task: asyncio.Task | None = None
        self.flush_errors = 0
        self.rows_written = 0

    async def start(self) -> None:
        import asyncpg  # optional dependency
        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=8)
        async with self._pool.acquire() as c:
            await c.execute("SELECT pg_advisory_lock(7101)")  # replicas starting together must not race on DDL
            try:
                await c.execute(SCHEMA)
            finally:
                await c.execute("SELECT pg_advisory_unlock(7101)")
        self._task = asyncio.get_running_loop().create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        try:
            await self.flush()
        except Exception:
            log.exception("final flush failed; %d request rows and %d batch rows not saved", len(self._req), len(self._batch))
        if self._pool:
            await self._pool.close()

    # ---- write path ------------------------------------------------------------------------------------------
    def put_request(self, row: tuple) -> None:
        self._req[row[0]] = row

    def put_batch(self, row: tuple) -> None:
        self._batch[row[0]] = row

    async def insert_batch(self, batch_row: tuple, req_rows: list[tuple]) -> None:
        """Durable insert, awaited before the batch is acknowledged. Raises on a duplicate id."""
        async with self._pool.acquire() as c, c.transaction():
            await c.execute(UPSERT_BATCHES, *[[v] for v in batch_row])
            for i in range(0, len(req_rows), self.max_flush_rows):
                cols = list(zip(*req_rows[i:i + self.max_flush_rows]))
                tag = await c.execute(INSERT_REQUESTS_NEW, *[list(col) for col in cols])
                if tag != f"INSERT 0 {len(cols[0])}":
                    raise DuplicateId("one or more request ids already exist in the database")

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self.flush_interval_s)
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.flush_errors += 1
                log.warning("flush failed (%s); will retry, %d request rows pending", e, len(self._req))

    async def flush(self) -> None:
        if not self._req and not self._batch:
            return
        reqs, self._req = self._req, {}
        batches, self._batch = self._batch, {}
        try:
            async with self._pool.acquire() as c:
                if batches:
                    cols = list(zip(*batches.values()))
                    await c.execute(UPSERT_BATCHES, *[list(col) for col in cols])
                rows = list(reqs.values())
                for i in range(0, len(rows), self.max_flush_rows):
                    cols = list(zip(*rows[i:i + self.max_flush_rows]))
                    await c.execute(UPSERT_REQUESTS, *[list(col) for col in cols])
            self.rows_written += len(reqs) + len(batches)
        except Exception:
            for k, v in reqs.items():  # put back, without overwriting anything newer
                self._req.setdefault(k, v)
            for k, v in batches.items():
                self._batch.setdefault(k, v)
            raise

    # ---- reads -----------------------------------------------------------------------------------------------
    async def get_request(self, rid: str) -> dict | None:
        await self.flush_quietly()
        async with self._pool.acquire() as c:
            row = await c.fetchrow("SELECT * FROM requests WHERE id = $1", rid)
        return dict(row) if row else None

    async def get_batch(self, bid: str) -> dict | None:
        await self.flush_quietly()
        async with self._pool.acquire() as c:
            row = await c.fetchrow("SELECT * FROM batches WHERE id = $1", bid)
            if row is None:
                return None
            states = await c.fetch("SELECT state, count(*) AS n FROM requests WHERE batch_id = $1 GROUP BY state", bid)
        return {**dict(row), "states": {r["state"]: r["n"] for r in states}}

    async def batch_results(self, bid: str, offset: int, limit: int) -> list[dict] | None:
        await self.flush_quietly()
        async with self._pool.acquire() as c:
            rows = await c.fetch("SELECT * FROM requests WHERE batch_id = $1 ORDER BY idx OFFSET $2 LIMIT $3", bid, offset, limit)
        return [dict(r) for r in rows]

    async def flush_quietly(self) -> None:
        """Make reads see this process's own pending writes."""
        try:
            await self.flush()
        except Exception:
            pass

    # ---- limits ----------------------------------------------------------------------------------------------
    async def limits(self) -> dict[str, tuple[int, int]]:
        async with self._pool.acquire() as c:
            return {r["model"]: (r["rpm"], r["tpm"]) for r in await c.fetch("SELECT * FROM model_limits")}

    async def save_limits(self, model: str, rpm: int, tpm: int) -> None:
        async with self._pool.acquire() as c:
            await c.execute("INSERT INTO model_limits (model, rpm, tpm, updated_at) VALUES ($1, $2, $3, $4) "
                            "ON CONFLICT (model) DO UPDATE SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm, updated_at = EXCLUDED.updated_at",
                            model, rpm, tpm, time.time())

    # ---- recovery --------------------------------------------------------------------------------------------
    async def owners_with_open_work(self) -> list[str]:
        async with self._pool.acquire() as c:
            rows = await c.fetch(
                "SELECT owner FROM batches WHERE status = 'processing' OR cb_status = ANY($1) "
                "UNION SELECT owner FROM requests WHERE state = ANY($2)", list(OPEN_CALLBACK), list(OPEN_STATES))
        return [r["owner"] for r in rows]

    async def claim(self, me: str, dead: list[str]) -> tuple[list[tuple[dict, list[dict]]], list[dict]]:
        """Take over unfinished batches and single requests of dead replicas.

        The UPDATEs are atomic per row, so two replicas claiming at once never both get the same work.
        Returns ([(batch_row, all its request rows in order)], [open single request rows]).
        """
        async with self._pool.acquire() as c, c.transaction():
            claimed = await c.fetch(
                "UPDATE batches SET owner = $1 WHERE owner = ANY($2) AND (status = 'processing' OR cb_status = ANY($3)) RETURNING *",
                me, dead, list(OPEN_CALLBACK))
            bids = [r["id"] for r in claimed]
            batches = []
            if bids:
                await c.execute("UPDATE requests SET owner = $1 WHERE batch_id = ANY($2)", me, bids)
                reqs = await c.fetch("SELECT * FROM requests WHERE batch_id = ANY($1) ORDER BY batch_id, idx", bids)
                by_batch: dict[str, list[dict]] = {b: [] for b in bids}
                for r in reqs:
                    by_batch[r["batch_id"]].append(dict(r))
                batches = [(dict(b), by_batch[b["id"]]) for b in claimed]
            singles = await c.fetch(
                "UPDATE requests SET owner = $1 WHERE batch_id IS NULL AND owner = ANY($2) AND state = ANY($3) RETURNING *",
                me, dead, list(OPEN_STATES))
        return batches, [dict(r) for r in singles]

    async def truncate(self) -> None:
        self._req.clear()
        self._batch.clear()
        async with self._pool.acquire() as c:
            await c.execute("TRUNCATE requests, batches")


class DuplicateId(Exception):
    pass


def encode_payload(payload) -> str:
    return json.dumps(payload)
