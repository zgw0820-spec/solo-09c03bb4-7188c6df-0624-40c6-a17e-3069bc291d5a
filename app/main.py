"""
机房虚拟机启动器 · 内网 IPv4 地址租约服务

- FastAPI + SQLite(WAL)。Docker Compose 启动两个实例，挂载同一命名卷，共享同一个数据库文件。
- 申请幂等：request_id（申请号）唯一；同号重试返回原租约的当前状态，过期后也不会借旧号再领地址。
- 分配原子性：所有写操作走 BEGIN IMMEDIATE 事务，跨实例串行化；
  “查空闲地址 + 写租约” 是原子操作，两个实例争抢最后一个地址时只有一方成功。
- 懒过期：expires_at <= now 即视为过期，地址立即可回收分配给其它虚拟机，无需后台清理线程。
- 令牌保护：续租/释放必须携带租约令牌；地址易主后，旧令牌迟到的续租/释放不影响新归属。
- 同一虚拟机在任意时刻只能持有一个有效（未释放且未过期）地址。
"""
from __future__ import annotations

import ipaddress
import os
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ---------------- 配置（环境变量） ----------------

DB_PATH = os.environ.get("DB_PATH", "./data/leases.db")
POOL_CIDR = os.environ.get("POOL_CIDR", "10.99.0.0/29")
INSTANCE_ID = os.environ.get("INSTANCE_ID", "standalone")
ALLOW_RESET = os.environ.get("ALLOW_RESET", "0") == "1"  # 仅自测用途
MIN_TTL = 1
MAX_TTL = 60
BUSY_TIMEOUT_MS = 15000


def _load_pool(cidr: str) -> list[str]:
    net = ipaddress.ip_network(cidr, strict=False)
    addrs = [str(ip) for ip in net.hosts()]
    if not addrs:  # /32 的边界情况
        addrs = [str(net.network_address)]
    return addrs


POOL_ADDRESSES = _load_pool(POOL_CIDR)

# ---------------- SQLite ----------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS leases (
    lease_id    TEXT PRIMARY KEY,        -- 租约 ID
    request_id  TEXT NOT NULL UNIQUE,    -- 申请号（幂等键）：一个申请号终身只对应一条租约
    vm_id       TEXT NOT NULL,           -- 虚拟机 ID
    address     TEXT NOT NULL,           -- 分到的 IPv4 地址
    token       TEXT NOT NULL,           -- 租约令牌：续租/释放凭证
    issued_at   REAL NOT NULL,           -- 签发时间（epoch 秒）
    expires_at  REAL NOT NULL,           -- 过期时间（epoch 秒）
    released_at REAL,                    -- 释放时间；NULL = 未释放
    node        TEXT NOT NULL            -- 处理该申请的实例
);
CREATE INDEX IF NOT EXISTS idx_leases_address ON leases(address);
CREATE INDEX IF NOT EXISTS idx_leases_vm_id  ON leases(vm_id);

CREATE TABLE IF NOT EXISTS request_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,            -- 申请号
    vm_id      TEXT NOT NULL,
    result     TEXT NOT NULL,            -- leased / replayed / rejected_*
    lease_id   TEXT,
    detail     TEXT,
    node       TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_request_log_req ON request_log(request_id);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")  # 先设忙等，再开 WAL
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE 写事务。

    两个实例共用同一个 SQLite 文件，BEGIN IMMEDIATE 立即取得写锁，
    把全部写事务在数据库层串行化，保证分配决策的原子性。
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


@contextmanager
def read_conn() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with read_conn() as conn:
        conn.executescript(SCHEMA)  # IF NOT EXISTS，两个实例并发启动也安全


# ---------------- 视图与工具 ----------------

def _iso(ts: Optional[float]) -> Optional[str]:
    return None if ts is None else datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _status_of(row: sqlite3.Row, now: float) -> str:
    if row["released_at"] is not None:
        return "released"
    if row["expires_at"] <= now:
        return "expired"
    return "active"


def _lease_view(row: sqlite3.Row, now: Optional[float] = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    status = _status_of(row, now)
    return {
        "lease_id": row["lease_id"],
        "request_id": row["request_id"],
        "vm_id": row["vm_id"],
        "address": row["address"],
        "token": row["token"],
        "status": status,
        "issued_at": _iso(row["issued_at"]),
        "expires_at": _iso(row["expires_at"]),
        "released_at": _iso(row["released_at"]),
        "remaining_seconds": round(max(0.0, row["expires_at"] - now), 3) if status == "active" else 0,
        "leased_via_node": row["node"],
    }


def _error(status_code: int, code: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(status_code=status_code, detail={"error": code, "message": message, **extra})


def _conflict(code: str, message: str, **extra: Any) -> HTTPException:
    return _error(409, code, message, **extra)


def _log_request(conn: sqlite3.Connection, body: "ApplyIn", result: str,
                 lease_id: Optional[str], detail: str) -> None:
    conn.execute(
        "INSERT INTO request_log (request_id, vm_id, result, lease_id, detail, node, created_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (body.request_id, body.vm_id, result, lease_id, detail, INSTANCE_ID, time.time()),
    )


# ---------------- 应用 ----------------

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="机房虚拟机地址租约服务",
    version="1.0.0",
    description=(
        "内网 IPv4 地址池租约 API。申请幂等（同申请号重试返回原租约当前状态）；"
        "同一虚拟机仅可持有一个有效地址；过期地址可回收再分配；"
        "续租/释放需携带租约令牌，旧令牌不影响新归属。"
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Served-By"] = INSTANCE_ID
    return response


class ApplyIn(BaseModel):
    """启动器提交的申请：虚拟机 ID + 申请号 + 租期。"""

    vm_id: str = Field(min_length=1, max_length=128, description="虚拟机 ID", examples=["vm-1001"])
    request_id: str = Field(min_length=1, max_length=128,
                            description="申请号（幂等键）：同号重试返回原租约当前状态", examples=["req-9f3a2c"])
    ttl_seconds: int = Field(ge=MIN_TTL, le=MAX_TTL, description="租期（秒），1~60", examples=[30])


class RenewIn(BaseModel):
    token: str = Field(min_length=1, max_length=128, description="申请时返回的租约令牌")
    ttl_seconds: int = Field(ge=MIN_TTL, le=MAX_TTL, description="从当前时刻起续租的秒数，1~60", examples=[30])


class ReleaseIn(BaseModel):
    token: str = Field(min_length=1, max_length=128, description="申请时返回的租约令牌")


# ---------------- 租约操作 ----------------

@app.post("/api/leases", tags=["租约"], summary="申请地址（幂等）", status_code=201)
def apply_lease(body: ApplyIn):
    """申请一个地址。同一申请号重试：返回原租约当前状态（200, replayed=true），
    即使原租约已过期也不会再分配新地址。"""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute("SELECT * FROM leases WHERE request_id = ?", (body.request_id,)).fetchone()
        if row is not None:
            if row["vm_id"] != body.vm_id:
                _log_request(conn, body, "rejected_request_id_conflict", row["lease_id"],
                             "申请号已被其它虚拟机占用")
                outcome: tuple[str, Any] = ("error", _conflict(
                    "request_id_conflict", "申请号已被其它虚拟机占用", lease=_lease_view(row, now)))
            else:
                _log_request(conn, body, "replayed", row["lease_id"], "同号重试，返回原租约当前状态")
                outcome = ("ok", JSONResponse(status_code=200, content={
                    "replayed": True,
                    "message": "相同申请号已存在，返回原租约当前状态（不重复分配；过期后也不会借旧号再领地址）",
                    "lease": _lease_view(row, now),
                }))
        else:
            held = conn.execute(
                "SELECT * FROM leases WHERE vm_id = ? AND released_at IS NULL AND expires_at > ?",
                (body.vm_id, now)).fetchone()
            if held is not None:
                _log_request(conn, body, "rejected_vm_already_leased", held["lease_id"],
                             "虚拟机已持有有效地址")
                outcome = ("error", _conflict(
                    "vm_already_leased", "同一虚拟机只能持有一个有效地址",
                    existing_lease=_lease_view(held, now)))
            else:
                busy = {r["address"] for r in conn.execute(
                    "SELECT address FROM leases WHERE released_at IS NULL AND expires_at > ?",
                    (now,)).fetchall()}
                free = [a for a in POOL_ADDRESSES if a not in busy]
                if not free:
                    _log_request(conn, body, "rejected_pool_exhausted", None, "地址不足")
                    outcome = ("error", _conflict("pool_exhausted", "地址不足：地址池已耗尽，请稍后重试"))
                else:
                    address = free[0]
                    lease_id = uuid.uuid4().hex
                    token = secrets.token_hex(16)
                    conn.execute(
                        "INSERT INTO leases (lease_id, request_id, vm_id, address, token,"
                        " issued_at, expires_at, node) VALUES (?,?,?,?,?,?,?,?)",
                        (lease_id, body.request_id, body.vm_id, address, token,
                         now, now + body.ttl_seconds, INSTANCE_ID))
                    _log_request(conn, body, "leased", lease_id, f"分配地址 {address}")
                    row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
                    outcome = ("ok", JSONResponse(status_code=201, content={
                        "replayed": False,
                        "message": "租约创建成功",
                        "lease": _lease_view(row, now),
                    }))
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


@app.post("/api/leases/{lease_id}/renew", tags=["租约"], summary="续租（需令牌）")
def renew_lease(lease_id: str, body: RenewIn):
    """从当前时刻起延长 ttl_seconds 秒。令牌不匹配、已释放、已过期的租约一律拒绝，
    且不影响地址的当前归属。"""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        if row is None:
            outcome = ("error", _error(404, "lease_not_found", "租约不存在"))
        elif row["token"] != body.token:
            outcome = ("error", _conflict(
                "token_mismatch",
                "令牌不匹配：该租约可能已过期并易主，迟到的续租已被拒绝，不影响当前归属",
                lease=_lease_view(row, now)))
        elif row["released_at"] is not None:
            outcome = ("error", _conflict("lease_released", "租约已释放，无法续租",
                                          lease=_lease_view(row, now)))
        elif row["expires_at"] <= now:
            outcome = ("error", _conflict(
                "lease_expired", "租约已过期，无法续租；过期地址可能已分配给其它虚拟机",
                lease=_lease_view(row, now)))
        else:
            conn.execute("UPDATE leases SET expires_at = ? WHERE lease_id = ?",
                         (now + body.ttl_seconds, lease_id))
            row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            outcome = ("ok", {"message": "续租成功", "lease": _lease_view(row, now)})
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


@app.post("/api/leases/{lease_id}/release", tags=["租约"], summary="释放（需令牌，幂等）")
def release_lease(lease_id: str, body: ReleaseIn):
    """释放租约。重复释放幂等返回；旧令牌迟到的释放只作用于已终止的旧租约，不影响新归属。"""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        if row is None:
            outcome = ("error", _error(404, "lease_not_found", "租约不存在"))
        elif row["token"] != body.token:
            outcome = ("error", _conflict(
                "token_mismatch",
                "令牌不匹配：该租约可能已过期并易主，迟到的释放已被拒绝，不影响当前归属",
                lease=_lease_view(row, now)))
        elif row["released_at"] is not None:
            outcome = ("ok", {"message": "租约此前已释放，重复释放幂等返回（无副作用）",
                              "lease": _lease_view(row, now)})
        else:
            conn.execute("UPDATE leases SET released_at = ? WHERE lease_id = ?", (now, lease_id))
            row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            outcome = ("ok", {"message": "释放成功", "lease": _lease_view(row, now)})
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


# ---------------- 查询 ----------------

@app.get("/api/leases/{lease_id}", tags=["查询"], summary="按租约 ID 查询状态")
def get_lease(lease_id: str):
    with read_conn() as conn:
        row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
    if row is None:
        raise _error(404, "lease_not_found", "租约不存在")
    return {"lease": _lease_view(row)}


@app.get("/api/requests/{request_id}", tags=["查询"], summary="按申请号查询租约与申请记录")
def get_request(request_id: str):
    with read_conn() as conn:
        row = conn.execute("SELECT * FROM leases WHERE request_id = ?", (request_id,)).fetchone()
        logs = conn.execute("SELECT * FROM request_log WHERE request_id = ? ORDER BY id",
                            (request_id,)).fetchall()
    if row is None and not logs:
        raise _error(404, "request_not_found", "申请号不存在")
    return {
        "request_id": request_id,
        "lease": _lease_view(row) if row is not None else None,
        "attempts": [
            {
                "seq": log["id"],
                "vm_id": log["vm_id"],
                "result": log["result"],
                "lease_id": log["lease_id"],
                "detail": log["detail"],
                "node": log["node"],
                "at": _iso(log["created_at"]),
            }
            for log in logs
        ],
    }


@app.get("/api/addresses/{address}", tags=["查询"], summary="查询地址当前归属与历史")
def get_address(address: str):
    if address not in POOL_ADDRESSES:
        raise _error(404, "address_not_in_pool", "该地址不在地址池中", pool_cidr=POOL_CIDR)
    now = time.time()
    with read_conn() as conn:
        current = conn.execute(
            "SELECT * FROM leases WHERE address = ? AND released_at IS NULL AND expires_at > ?"
            " ORDER BY issued_at DESC LIMIT 1", (address, now)).fetchone()
        history = conn.execute(
            "SELECT * FROM leases WHERE address = ? ORDER BY issued_at DESC LIMIT 20",
            (address,)).fetchall()
    return {
        "address": address,
        "pool_cidr": POOL_CIDR,
        "state": "leased" if current is not None else "free",
        "current_holder": _lease_view(current, now) if current is not None else None,
        "history": [_lease_view(r, now) for r in history],
    }


@app.get("/api/leases", tags=["查询"], summary="租约台账（可按虚拟机/状态过滤）")
def list_leases(
    vm_id: Optional[str] = Query(default=None, description="按虚拟机 ID 过滤"),
    lease_status: Optional[str] = Query(default=None, alias="status",
                                        pattern="^(active|expired|released)$",
                                        description="按状态过滤：active/expired/released"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    with read_conn() as conn:
        rows = conn.execute("SELECT * FROM leases ORDER BY issued_at DESC, lease_id DESC LIMIT ?",
                            (limit,)).fetchall()
    now = time.time()
    views = [_lease_view(r, now) for r in rows]
    if vm_id is not None:
        views = [v for v in views if v["vm_id"] == vm_id]
    if lease_status is not None:
        views = [v for v in views if v["status"] == lease_status]
    return {"count": len(views), "leases": views}


@app.get("/api/pool", tags=["查询"], summary="地址池总览")
def pool_status():
    now = time.time()
    with read_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM leases WHERE released_at IS NULL AND expires_at > ?", (now,)).fetchall()
    active = {r["address"]: r for r in rows}
    addresses = []
    for addr in POOL_ADDRESSES:
        r = active.get(addr)
        addresses.append({
            "address": addr,
            "state": "leased" if r is not None else "free",
            "vm_id": r["vm_id"] if r is not None else None,
            "lease_id": r["lease_id"] if r is not None else None,
            "expires_at": _iso(r["expires_at"]) if r is not None else None,
        })
    leased = sum(1 for a in addresses if a["state"] == "leased")
    return {
        "pool_cidr": POOL_CIDR,
        "total": len(addresses),
        "leased": leased,
        "free": len(addresses) - leased,
        "addresses": addresses,
    }


# ---------------- 系统 ----------------

@app.get("/api/health", tags=["系统"], summary="健康检查")
def health():
    with read_conn() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok", "node": INSTANCE_ID, "time": _iso(time.time())}


@app.get("/", tags=["系统"], summary="服务信息")
def root():
    return {
        "service": "机房虚拟机地址租约服务",
        "node": INSTANCE_ID,
        "docs": "/docs",
        "pool_cidr": POOL_CIDR,
        "pool_size": len(POOL_ADDRESSES),
        "ttl_range_seconds": [MIN_TTL, MAX_TTL],
    }


@app.post("/api/admin/reset", tags=["系统"], summary="清空全部数据（仅自测，需 ALLOW_RESET=1）")
def admin_reset():
    if not ALLOW_RESET:
        raise _error(403, "reset_disabled", "未启用重置功能（设 ALLOW_RESET=1 开启，仅供自测）")
    with write_tx() as conn:
        conn.execute("DELETE FROM leases")
        conn.execute("DELETE FROM request_log")
    return {"message": "已清空全部租约与申请记录", "node": INSTANCE_ID}
