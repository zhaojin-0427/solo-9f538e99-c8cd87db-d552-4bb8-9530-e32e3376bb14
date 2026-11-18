"""SQLite 持久化层：模式、事务性批量入库、过程目录重算与查询。

设计要点：
- readings 以 (station, device, minute_epoch) 为主键，同键只接受更高修订号；
- 每批上传在单个事务中写入（读数 + 批次记录 + 行级明细 + 受影响日期的过程目录），
  任何异常整体回滚，不会留下半批新结果；
- 过程目录（processes/process_links）是持久化投影：上传事务内对受影响的
  （站点, UTC 自然日）重算分段；起止与累计雨量未变的过程沿用编号，变化的旧编号
  置为 superseded 并保留原结果，按共有降雨小时与新编号建立直接替代关联；
- 数据库文件落在磁盘（Docker 中挂载卷），重启后新旧编号仍可查询。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from . import core

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    station      TEXT    NOT NULL,
    device       TEXT    NOT NULL CHECK (device IN ('primary', 'backup')),
    minute_epoch INTEGER NOT NULL,
    counter      INTEGER NOT NULL CHECK (counter BETWEEN 0 AND 9999),
    revision     INTEGER NOT NULL CHECK (revision >= 0),
    updated_at   TEXT    NOT NULL,
    PRIMARY KEY (station, device, minute_epoch)
);
CREATE INDEX IF NOT EXISTS idx_readings_station_minute
    ON readings (station, minute_epoch);

CREATE TABLE IF NOT EXISTS batches (
    batch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    uploaded_at   TEXT NOT NULL,
    filename      TEXT,
    total_rows    INTEGER NOT NULL,
    accepted_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_rows (
    batch_id INTEGER NOT NULL REFERENCES batches (batch_id) ON DELETE CASCADE,
    row_no   INTEGER NOT NULL,
    outcome  TEXT NOT NULL CHECK (outcome IN ('accepted', 'rejected')),
    detail   TEXT,
    raw      TEXT
);
CREATE INDEX IF NOT EXISTS idx_batch_rows_batch ON batch_rows (batch_id);

-- 降雨过程目录：每个编号一行，superseded 行永久保留原结果供追查
CREATE TABLE IF NOT EXISTS processes (
    code             TEXT PRIMARY KEY,          -- 编号：站点-YYYYMMDD-序号
    station          TEXT NOT NULL,
    day_epoch        INTEGER NOT NULL,          -- 所属 UTC 自然日 00:00Z
    seq              INTEGER NOT NULL,          -- 站点+日 内递增序号，不复用
    start_hour       INTEGER NOT NULL,          -- 首小时起始 epoch
    end_hour         INTEGER NOT NULL,          -- 末小时起始 epoch（结束时刻 = end_hour+3600）
    hours            INTEGER NOT NULL CHECK (hours > 0),
    total_pulses     INTEGER NOT NULL CHECK (total_pulses > 0),
    rainfall_mm      REAL NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('active', 'superseded')),
    created_batch    INTEGER NOT NULL REFERENCES batches (batch_id),
    created_at       TEXT NOT NULL,
    superseded_batch INTEGER REFERENCES batches (batch_id),
    superseded_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_processes_day
    ON processes (station, day_epoch, status);
CREATE INDEX IF NOT EXISTS idx_processes_created ON processes (created_batch);
CREATE INDEX IF NOT EXISTS idx_processes_superseded ON processes (superseded_batch);

-- 直接替代关联：旧编号 -> 新编号，按共有降雨小时建立
CREATE TABLE IF NOT EXISTS process_links (
    old_code     TEXT NOT NULL REFERENCES processes (code),
    new_code     TEXT NOT NULL REFERENCES processes (code),
    shared_hours INTEGER NOT NULL CHECK (shared_hours > 0),
    batch_id     INTEGER NOT NULL REFERENCES batches (batch_id),
    PRIMARY KEY (old_code, new_code)
);
CREATE INDEX IF NOT EXISTS idx_process_links_new ON process_links (new_code);
CREATE INDEX IF NOT EXISTS idx_process_links_batch ON process_links (batch_id);
"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: str) -> sqlite3.Connection:
    """新建连接。autocommit 模式，写操作由 apply_batch 显式开事务。"""
    conn = sqlite3.connect(db_path, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def apply_batch(conn: sqlite3.Connection, valid_rows, row_errors, filename=None) -> dict:
    """在同一事务中应用一批读数并重算受影响日期的过程目录。

    读数、批次记录、行级明细与过程目录变更同属一个事务：任何一步失败
    （包括目录重算失败）整体回滚，不会留下半批新结果。
    若调用方已开启事务（如请求级快照），则以 SAVEPOINT 保证本批原子性；
    否则自行 BEGIN IMMEDIATE。同键（站点,设备,分钟）只接受更高修订号，
    否则记为拒绝。返回批次报告 dict（含 catalog_changes）。
    """
    now = _utcnow_iso()
    accepted = []
    rejected = [{"row_no": e.row_no, "reason": e.reason, "raw": e.raw} for e in row_errors]
    total = len(valid_rows) + len(row_errors)

    own_txn = not conn.in_transaction
    if own_txn:
        conn.execute("BEGIN IMMEDIATE")
    else:
        conn.execute("SAVEPOINT apply_batch")
    try:
        cur = conn.execute(
            "INSERT INTO batches (uploaded_at, filename, total_rows, accepted_rows, rejected_rows)"
            " VALUES (?, ?, ?, 0, 0)",
            (now, filename, total),
        )
        batch_id = cur.lastrowid

        affected_days = set()  # {(station, day_epoch)}：被本批接受读数影响的目录日
        for row_no, raw, r in valid_rows:
            existing = conn.execute(
                "SELECT revision FROM readings"
                " WHERE station=? AND device=? AND minute_epoch=?",
                (r.station, r.device, r.minute_epoch),
            ).fetchone()
            if existing is not None and r.revision <= existing["revision"]:
                rejected.append({
                    "row_no": row_no,
                    "reason": f"修订号 {r.revision} 不高于已存的 {existing['revision']}，同键只接受更高修订",
                    "raw": raw,
                })
                continue
            if existing is None:
                conn.execute(
                    "INSERT INTO readings (station, device, minute_epoch, counter, revision, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (r.station, r.device, r.minute_epoch, r.counter, r.revision, now),
                )
                action = "inserted"
            else:
                conn.execute(
                    "UPDATE readings SET counter=?, revision=?, updated_at=?"
                    " WHERE station=? AND device=? AND minute_epoch=?",
                    (r.counter, r.revision, now, r.station, r.device, r.minute_epoch),
                )
                action = "updated"
            accepted.append({"row_no": row_no, "action": action, "reading": r})
            conn.execute(
                "INSERT INTO batch_rows (batch_id, row_no, outcome, detail, raw)"
                " VALUES (?, ?, 'accepted', ?, ?)",
                (batch_id, row_no, action, raw),
            )
            for day_epoch in core.affected_days_for_minute(r.minute_epoch):
                affected_days.add((r.station, day_epoch))

        for rej in rejected:
            conn.execute(
                "INSERT INTO batch_rows (batch_id, row_no, outcome, detail, raw)"
                " VALUES (?, ?, 'rejected', ?, ?)",
                (batch_id, rej["row_no"], rej["reason"], rej["raw"]),
            )

        conn.execute(
            "UPDATE batches SET accepted_rows=?, rejected_rows=? WHERE batch_id=?",
            (len(accepted), len(rejected), batch_id),
        )

        # 目录重算与读数同一事务：受影响日期的目录与小时雨量按同一批读数生效
        catalog_changes = reconcile_catalog(conn, affected_days, batch_id, now)

        if own_txn:
            conn.execute("COMMIT")
        else:
            conn.execute("RELEASE apply_batch")
    except Exception:
        if own_txn:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        else:
            try:
                conn.execute("ROLLBACK TO apply_batch")
                conn.execute("RELEASE apply_batch")
            except sqlite3.Error:
                pass
        raise

    rejected.sort(key=lambda x: x["row_no"])
    return {
        "batch_id": batch_id,
        "filename": filename,
        "uploaded_at": now,
        "total_rows": total,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "accepted": accepted,
        "rejected": rejected,
        "catalog_changes": catalog_changes,
    }


# ---------------- 过程目录 ----------------

def fetch_range_counters(conn: sqlite3.Connection, station: str,
                         start_epoch: int, end_epoch: int) -> dict:
    """取某站 [start_epoch, end_epoch] 范围内全部读数（含两端）。"""
    rows = conn.execute(
        "SELECT device, minute_epoch, counter FROM readings"
        " WHERE station=? AND minute_epoch BETWEEN ? AND ?",
        (station, start_epoch, end_epoch),
    ).fetchall()
    return {(r["device"], r["minute_epoch"]): r["counter"] for r in rows}


def _day_segments(conn: sqlite3.Connection, station: str, day_epoch: int) -> list:
    """按当前 readings 重算某站某 UTC 自然日的降雨过程段（与小时雨量同一计算入口）。"""
    counters = fetch_range_counters(conn, station, day_epoch, day_epoch + core.DAY_SECONDS)
    hourly = []
    for i in range(24):
        h = day_epoch + i * core.HOUR_SECONDS
        r = core.compute_hour(counters, station, h)
        hourly.append({
            "hour_start": h,
            "ok": r["status"] == "ok",
            "pulses": r["total_pulses"],
        })
    return core.segment_day(hourly)


def _process_json_row(row) -> dict:
    """processes 表行 -> 报告/JSON 用 dict（end 为过程结束时刻）。"""
    return {
        "code": row["code"],
        "start": core.iso_utc(row["start_hour"]),
        "end": core.iso_utc(row["end_hour"] + core.HOUR_SECONDS),
        "hours": row["hours"],
        "total_pulses": row["total_pulses"],
        "rainfall_mm": row["rainfall_mm"],
    }


def reconcile_catalog(conn: sqlite3.Connection, station_days, batch_id: int, now: str) -> list:
    """对受影响的 (站点, UTC 自然日) 重算过程目录，须在读数写入的同一事务内调用。

    规则：
    - 起止及累计雨量（总脉冲）未变的过程沿用编号；
    - 变化的旧编号置为 superseded，保留原结果；
    - 新分段发新编号（站点+日内序号递增，永不复用）；
    - 旧编号与新编号按共有降雨小时建立直接替代关联（拆分、合并、消失均可查）。
    返回每个 (站点, 日) 的变更说明（kept/created/superseded），供批次报告使用。
    """
    changes = []
    for station, day_epoch in sorted(station_days):
        segments = _day_segments(conn, station, day_epoch)
        old_active = conn.execute(
            "SELECT * FROM processes WHERE station=? AND day_epoch=? AND status='active'"
            " ORDER BY start_hour",
            (station, day_epoch),
        ).fetchall()

        # 1) 起止与累计雨量一致 -> 沿用编号
        kept_codes = set()
        for seg in segments:
            seg["kept"] = False
            for row in old_active:
                if row["code"] in kept_codes:
                    continue
                if (row["start_hour"] == seg["start_hour"]
                        and row["end_hour"] == seg["end_hour"]
                        and row["total_pulses"] == seg["total_pulses"]):
                    kept_codes.add(row["code"])
                    seg["kept"] = True
                    seg["code"] = row["code"]
                    break

        # 2) 新分段发新编号
        seq = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM processes WHERE station=? AND day_epoch=?",
            (station, day_epoch),
        ).fetchone()["m"]
        date_text = core.utc_date(day_epoch)
        for seg in segments:
            if seg["kept"]:
                continue
            seq += 1
            code = f"{station}-{date_text.replace('-', '')}-{seq}"
            conn.execute(
                "INSERT INTO processes (code, station, day_epoch, seq, start_hour, end_hour,"
                " hours, total_pulses, rainfall_mm, status, created_batch, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)",
                (code, station, day_epoch, seq, seg["start_hour"], seg["end_hour"],
                 seg["hours"], seg["total_pulses"], seg["rainfall_mm"], batch_id, now),
            )
            seg["code"] = code

        # 3) 未沿用的旧编号置为 superseded（保留原结果）
        superseded_rows = [r for r in old_active if r["code"] not in kept_codes]
        for row in superseded_rows:
            conn.execute(
                "UPDATE processes SET status='superseded', superseded_batch=?, superseded_at=?"
                " WHERE code=?",
                (batch_id, now, row["code"]),
            )

        # 4) 旧编号 -> 新编号：按共有降雨小时建立直接替代关联
        new_segments = [s for s in segments if not s["kept"]]
        superseded = []
        for row in superseded_rows:
            replacements = []
            for seg in new_segments:
                lo = max(row["start_hour"], seg["start_hour"])
                hi = min(row["end_hour"], seg["end_hour"])
                shared = (hi - lo) // core.HOUR_SECONDS + 1 if hi >= lo else 0
                if shared > 0:
                    conn.execute(
                        "INSERT INTO process_links (old_code, new_code, shared_hours, batch_id)"
                        " VALUES (?, ?, ?, ?)",
                        (row["code"], seg["code"], shared, batch_id),
                    )
                    replacements.append({"code": seg["code"], "shared_hours": shared})
            entry = _process_json_row(row)
            entry["replacements"] = replacements
            superseded.append(entry)

        kept = [s["code"] for s in segments if s["kept"]]
        created = [{
            "code": s["code"],
            "start": core.iso_utc(s["start_hour"]),
            "end": core.iso_utc(s["end_hour"] + core.HOUR_SECONDS),
            "hours": s["hours"],
            "total_pulses": s["total_pulses"],
            "rainfall_mm": s["rainfall_mm"],
        } for s in new_segments]
        if kept or created or superseded:
            changes.append({
                "station": station,
                "date": date_text,
                "kept": kept,
                "created": created,
                "superseded": superseded,
            })
    return changes


def list_active_processes(conn: sqlite3.Connection, station: str, day_epoch: int):
    """某站某 UTC 自然日的当前过程目录（按开始时间排序）。"""
    return conn.execute(
        "SELECT * FROM processes WHERE station=? AND day_epoch=? AND status='active'"
        " ORDER BY start_hour",
        (station, day_epoch),
    ).fetchall()


def list_superseded_processes(conn: sqlite3.Connection, station: str, day_epoch: int):
    """某站某 UTC 自然日已被替代的历史编号（最近的在前）。"""
    return conn.execute(
        "SELECT * FROM processes WHERE station=? AND day_epoch=? AND status='superseded'"
        " ORDER BY superseded_batch DESC, start_hour",
        (station, day_epoch),
    ).fetchall()


def get_process(conn: sqlite3.Connection, code: str):
    """按编号查过程（含已替代的历史编号）；不存在返回 None。"""
    return conn.execute(
        "SELECT * FROM processes WHERE code=?", (code,),
    ).fetchone()


def get_successors(conn: sqlite3.Connection, code: str):
    """某编号的直接替代过程（含共有降雨小时数）。"""
    return conn.execute(
        "SELECT p.*, l.shared_hours AS shared_hours, l.batch_id AS link_batch"
        " FROM process_links l JOIN processes p ON p.code = l.new_code"
        " WHERE l.old_code=? ORDER BY p.start_hour",
        (code,),
    ).fetchall()


def get_predecessors(conn: sqlite3.Connection, code: str):
    """某编号直接替代了哪些旧过程（含共有降雨小时数）。"""
    return conn.execute(
        "SELECT p.*, l.shared_hours AS shared_hours, l.batch_id AS link_batch"
        " FROM process_links l JOIN processes p ON p.code = l.old_code"
        " WHERE l.new_code=? ORDER BY p.start_hour",
        (code,),
    ).fetchall()


def catalog_changes_for_batch(conn: sqlite3.Connection, batch_id: int) -> list:
    """按 (站点, 日) 重建某批次的目录变更（供历史批次页展示）。"""
    created = conn.execute(
        "SELECT * FROM processes WHERE created_batch=? ORDER BY station, day_epoch, start_hour",
        (batch_id,),
    ).fetchall()
    superseded = conn.execute(
        "SELECT * FROM processes WHERE superseded_batch=? ORDER BY station, day_epoch, start_hour",
        (batch_id,),
    ).fetchall()
    links = conn.execute(
        "SELECT l.old_code, l.new_code, l.shared_hours FROM process_links l"
        " WHERE l.batch_id=?",
        (batch_id,),
    ).fetchall()
    repl_by_old = {}
    for lnk in links:
        repl_by_old.setdefault(lnk["old_code"], []).append(
            {"code": lnk["new_code"], "shared_hours": lnk["shared_hours"]}
        )
    days = {}
    for row in created:
        key = (row["station"], row["day_epoch"])
        days.setdefault(key, {"created": [], "superseded": []})["created"].append(
            _process_json_row(row)
        )
    for row in superseded:
        key = (row["station"], row["day_epoch"])
        entry = _process_json_row(row)
        entry["replacements"] = repl_by_old.get(row["code"], [])
        days.setdefault(key, {"created": [], "superseded": []})["superseded"].append(entry)
    return [{
        "station": station,
        "date": core.utc_date(day_epoch),
        "created": v["created"],
        "superseded": v["superseded"],
    } for (station, day_epoch), v in sorted(days.items())]


def fetch_hour_counters(conn: sqlite3.Connection, station: str, hour_start_epoch: int) -> dict:
    """取某站某小时 61 个端点（含下一整点）的全部读数。"""
    rows = conn.execute(
        "SELECT device, minute_epoch, counter FROM readings"
        " WHERE station=? AND minute_epoch BETWEEN ? AND ?",
        (station, hour_start_epoch, hour_start_epoch + 3600),
    ).fetchall()
    return {(r["device"], r["minute_epoch"]): r["counter"] for r in rows}


def list_stations(conn: sqlite3.Connection):
    return conn.execute(
        "SELECT station, COUNT(*) AS n, MIN(minute_epoch) AS first_minute,"
        " MAX(minute_epoch) AS last_minute"
        " FROM readings GROUP BY station ORDER BY station"
    ).fetchall()


def list_batches(conn: sqlite3.Connection, limit: int = 20):
    return conn.execute(
        "SELECT * FROM batches ORDER BY batch_id DESC LIMIT ?", (limit,)
    ).fetchall()


def get_batch(conn: sqlite3.Connection, batch_id: int):
    batch = conn.execute(
        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
    ).fetchone()
    if batch is None:
        return None, []
    rows = conn.execute(
        "SELECT row_no, outcome, detail, raw FROM batch_rows"
        " WHERE batch_id=? ORDER BY row_no",
        (batch_id,),
    ).fetchall()
    return batch, rows
