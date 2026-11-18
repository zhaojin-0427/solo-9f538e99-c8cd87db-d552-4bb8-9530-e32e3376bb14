"""Flask 网页与查询 API。

一致性设计：
- 网页展示与 API 使用同一个 compute_hour_result() / 同一批查询函数，两边结果一致；
- 小时雨量由 readings 表现算现算；过程目录在上传事务内重算并持久化，
  目录与小时雨量按同一批已提交读数生效；
- 每个请求在一个读快照中完成全部查询（请求级事务），网页与 API 不会新旧混用；
- 上传的读数、批次记录与目录变更同属一个事务，失败整体回滚，不留半批新结果。
"""
from __future__ import annotations

import os

from flask import (
    Flask, flash, g, jsonify, redirect, render_template, request, url_for,
)

from . import core, db


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path or os.environ.get("RAIN_DB", "rainfall.db")
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 上传上限 16MB
    app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

    # 启动时建库建表（幂等），保证重启后可直接查询
    conn = db.connect(app.config["DB_PATH"])
    try:
        db.init_db(conn)
    finally:
        conn.close()

    def get_conn() -> db.sqlite3.Connection:
        """请求级连接：首个查询开启事务快照，请求内全部读取看到同一状态。"""
        if "db_conn" not in g:
            conn = db.connect(app.config["DB_PATH"])
            conn.execute("BEGIN")  # 请求级快照：网页/API 单次响应不新旧混用
            g.db_conn = conn
        return g.db_conn

    @app.teardown_appcontext
    def _close_conn(exc):
        conn = g.pop("db_conn", None)
        if conn is not None:
            try:
                if conn.in_transaction:
                    if exc is not None:
                        conn.execute("ROLLBACK")
                    else:
                        conn.execute("COMMIT")
            finally:
                conn.close()

    @app.template_filter("iso")
    def _iso_filter(epoch):
        return core.iso_utc(int(epoch))

    @app.template_filter("date")
    def _date_filter(epoch):
        return core.utc_date(int(epoch))

    def compute_hour_result(station: str, hour_epoch: int) -> dict:
        """网页与 API 共用的小时计算入口。"""
        counters = db.fetch_hour_counters(get_conn(), station, hour_epoch)
        return core.compute_hour(counters, station, hour_epoch)

    def day_payload(station: str, day_epoch: int):
        """某站某 UTC 自然日的过程目录 + 逐小时雨量（同一快照，供网页与 API）。"""
        conn = get_conn()
        processes = db.list_active_processes(conn, station, day_epoch)
        hour_proc = {}
        for p in processes:
            h = p["start_hour"]
            while h <= p["end_hour"]:
                hour_proc[h] = p["code"]
                h += core.HOUR_SECONDS
        counters = db.fetch_range_counters(
            conn, station, day_epoch, day_epoch + core.DAY_SECONDS)
        hours = []
        for i in range(24):
            h = day_epoch + i * core.HOUR_SECONDS
            r = core.compute_hour(counters, station, h)
            hours.append({
                "hour_start": h,
                "status": r["status"],
                "rainfall_mm": r["rainfall_mm"],
                "process": hour_proc.get(h),
            })
        return processes, hours

    def read_upload_text():
        """从 multipart 文件或文本框读取 CSV 文本，返回 (text, filename)。"""
        file = request.files.get("file")
        if file is not None and file.filename:
            try:
                return file.read().decode("utf-8-sig"), file.filename
            except UnicodeDecodeError:
                raise core.CsvFormatError("文件不是有效的 UTF-8 编码")
        text = request.form.get("csv_text", "")
        if text.strip():
            return text, None
        raise core.CsvFormatError("请选择 CSV 文件或粘贴 CSV 文本")

    # ---------------- 网页 ----------------

    @app.get("/")
    def index():
        conn = get_conn()
        return render_template(
            "index.html",
            stations=db.list_stations(conn),
            batches=db.list_batches(conn),
        )

    @app.post("/upload")
    def upload_html():
        try:
            text, filename = read_upload_text()
            valid, errors = core.parse_csv_text(text)
        except core.CsvFormatError as exc:
            flash(f"上传失败：{exc}", "error")
            return redirect(url_for("index"))
        report = db.apply_batch(get_conn(), valid, errors, filename)
        return render_template("batch.html", report=report)

    @app.get("/report")
    def report_html():
        station = request.args.get("station", "").strip()
        hour_text = request.args.get("hour", "").strip()
        if not station or not hour_text:
            flash("请填写站点和整点小时", "error")
            return redirect(url_for("index"))
        try:
            hour = core.parse_hour(hour_text)
        except ValueError as exc:
            flash(f"小时参数无效：{exc}", "error")
            return redirect(url_for("index"))
        result = compute_hour_result(station, hour)
        return render_template(
            "report.html",
            r=result,
            prev_hour=core.iso_utc(hour - core.HOUR_SECONDS),
            next_hour=core.iso_utc(hour + core.HOUR_SECONDS),
        )

    @app.get("/day")
    def day_html():
        station = request.args.get("station", "").strip()
        date_text = request.args.get("date", "").strip()
        if not station or not date_text:
            flash("请填写站点和 UTC 日期", "error")
            return redirect(url_for("index"))
        try:
            day_epoch = core.parse_date(date_text)
        except ValueError as exc:
            flash(f"日期参数无效：{exc}", "error")
            return redirect(url_for("index"))
        processes, hours = day_payload(station, day_epoch)
        return render_template(
            "day.html",
            station=station,
            date=core.utc_date(day_epoch),
            prev_date=core.utc_date(day_epoch - core.DAY_SECONDS),
            next_date=core.utc_date(day_epoch + core.DAY_SECONDS),
            processes=processes,
            hours=hours,
            history=db.list_superseded_processes(get_conn(), station, day_epoch),
        )

    @app.get("/process/<path:code>")
    def process_html(code):
        conn = get_conn()
        p = db.get_process(conn, code)
        if p is None:
            flash(f"过程编号不存在：{code}", "error")
            return redirect(url_for("index"))
        return render_template(
            "process.html",
            p=p,
            successors=db.get_successors(conn, code),
            predecessors=db.get_predecessors(conn, code),
        )

    @app.get("/batches/<int:batch_id>")
    def batch_detail(batch_id):
        conn = get_conn()
        batch, rows = db.get_batch(conn, batch_id)
        if batch is None:
            flash("批次不存在", "error")
            return redirect(url_for("index"))
        changes = db.catalog_changes_for_batch(conn, batch_id)
        return render_template(
            "batch_detail.html", batch=batch, rows=rows, changes=changes)

    # ---------------- API（与网页同一计算入口，结果一致） ----------------

    @app.post("/api/upload")
    def upload_api():
        try:
            text, filename = read_upload_text()
            valid, errors = core.parse_csv_text(text)
        except core.CsvFormatError as exc:
            return jsonify({"error": str(exc)}), 400
        report = db.apply_batch(get_conn(), valid, errors, filename)
        return jsonify(_report_json(report))

    @app.get("/api/hourly")
    def hourly_api():
        station = request.args.get("station", "").strip()
        if not station:
            return jsonify({"error": "缺少 station 参数"}), 400
        try:
            hour = core.parse_hour(request.args.get("hour", ""))
        except ValueError as exc:
            return jsonify({"error": f"hour 参数无效: {exc}"}), 400
        return jsonify(compute_hour_result(station, hour))

    @app.get("/api/processes")
    def processes_api():
        station = request.args.get("station", "").strip()
        if not station:
            return jsonify({"error": "缺少 station 参数"}), 400
        try:
            day_epoch = core.parse_date(request.args.get("date", ""))
        except ValueError as exc:
            return jsonify({"error": f"date 参数无效: {exc}"}), 400
        processes, hours = day_payload(station, day_epoch)
        return jsonify({
            "station": station,
            "date": core.utc_date(day_epoch),
            "processes": [_process_json(p) for p in processes],
            "hours": [{
                "hour_start": core.iso_utc(h["hour_start"]),
                "status": h["status"],
                "rainfall_mm": h["rainfall_mm"],
                "process": h["process"],
            } for h in hours],
        })

    @app.get("/api/process/<path:code>")
    def process_api(code):
        conn = get_conn()
        p = db.get_process(conn, code)
        if p is None:
            return jsonify({"error": f"过程编号不存在: {code}"}), 404
        out = _process_json(p)
        out["created_at"] = p["created_at"]
        out["superseded_at"] = p["superseded_at"]
        out["replacements"] = [
            {"code": s["code"], "shared_hours": s["shared_hours"],
             "status": s["status"], "start": core.iso_utc(s["start_hour"]),
             "end": core.iso_utc(s["end_hour"] + core.HOUR_SECONDS),
             "rainfall_mm": s["rainfall_mm"]}
            for s in db.get_successors(conn, code)
        ]
        out["predecessors"] = [
            {"code": s["code"], "shared_hours": s["shared_hours"],
             "status": s["status"], "start": core.iso_utc(s["start_hour"]),
             "end": core.iso_utc(s["end_hour"] + core.HOUR_SECONDS),
             "rainfall_mm": s["rainfall_mm"]}
            for s in db.get_predecessors(conn, code)
        ]
        return jsonify(out)

    @app.get("/api/stations")
    def stations_api():
        out = [{
            "station": r["station"],
            "readings": r["n"],
            "first_minute": core.iso_utc(r["first_minute"]),
            "last_minute": core.iso_utc(r["last_minute"]),
        } for r in db.list_stations(get_conn())]
        return jsonify(out)

    @app.get("/api/batches")
    def batches_api():
        return jsonify([dict(r) for r in db.list_batches(get_conn(), limit=100)])

    @app.get("/api/health")
    def health():
        get_conn().execute("SELECT 1")
        return jsonify({"status": "ok"})

    @app.errorhandler(413)
    def too_large(_):
        return "上传文件过大（上限 16MB）", 413

    return app


def _process_json(p) -> dict:
    """processes 表行 -> 对外 JSON（end 为过程结束时刻）。"""
    return {
        "code": p["code"],
        "station": p["station"],
        "date": core.utc_date(p["day_epoch"]),
        "start": core.iso_utc(p["start_hour"]),
        "end": core.iso_utc(p["end_hour"] + core.HOUR_SECONDS),
        "hours": p["hours"],
        "total_pulses": p["total_pulses"],
        "rainfall_mm": p["rainfall_mm"],
        "status": p["status"],
        "created_batch": p["created_batch"],
        "superseded_batch": p["superseded_batch"],
    }


def _report_json(report: dict) -> dict:
    return {
        "batch_id": report["batch_id"],
        "filename": report["filename"],
        "uploaded_at": report["uploaded_at"],
        "total_rows": report["total_rows"],
        "accepted_count": report["accepted_count"],
        "rejected_count": report["rejected_count"],
        "accepted": [{
            "row_no": a["row_no"],
            "action": a["action"],
            "station": a["reading"].station,
            "device": a["reading"].device,
            "minute": core.iso_utc(a["reading"].minute_epoch),
            "counter": a["reading"].counter,
            "revision": a["reading"].revision,
        } for a in report["accepted"]],
        "rejected": report["rejected"],
        "catalog_changes": report["catalog_changes"],
    }


def main():
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)


if __name__ == "__main__":
    main()
