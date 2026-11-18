"""降雨过程目录自测：分段规则、编号沿用、替代关联（拆分/合并/消失）、
整点修订影响相邻两日、目录与读数同事务原子性、网页/API 一致性、重启持久化。

运行：python3 -m unittest discover -s tests -v
"""
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import core, db  # noqa: E402
from app.app import create_app  # noqa: E402


def epoch(y, mo, d, h=0, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


DAY1 = epoch(2026, 11, 17)   # 2026-11-17 00:00Z
DAY2 = epoch(2026, 11, 18)   # 2026-11-18 00:00Z


def day_csv(station, day, rainy_hours, rev=1, base=1000, per_min=2, device="primary"):
    """生成某日 00:00 ~ 次日 00:00 共 1441  行读数；rainy_hours(0..23) 内每分钟 +per_min，
    其余小时计数持平（零雨量）。"""
    lines, c = [], base
    for i in range(1441):
        m = day + i * 60
        if i > 0:
            hour_idx = (m - 60 - day) // 3600
            if hour_idx in rainy_hours:
                c += per_min
        lines.append(f"{station},{device},{core.iso_utc(m)},{c},{rev}")
    return "\n".join(lines)


def span_csv(station, start_day, rainy_by_day, rev=1, base=1000, per_min=2):
    """跨日读数：start_day 00:00 起共 len(rainy_by_day) 天；rainy_by_day={day_epoch:{小时}}。"""
    days = sorted(rainy_by_day)
    lines, c = [], base
    for i in range(len(days) * 1440 + 1):
        m = start_day + i * 60
        if i > 0:
            prev = m - 60
            d = prev - (prev % 86400)
            if (prev - d) // 3600 in rainy_by_day.get(d, set()):
                c += per_min
        lines.append(f"{station},primary,{core.iso_utc(m)},{c},{rev}")
    return "\n".join(lines)


class CoreSegmentTests(unittest.TestCase):
    """分段纯逻辑：连续有雨合成、零雨量/不可供数断开。"""

    def _hours(self, rainy=(), incomplete=()):
        out = []
        for i in range(24):
            ok = i not in incomplete
            out.append({
                "hour_start": DAY1 + i * 3600,
                "ok": ok,
                "pulses": (120 if i in rainy else 0) if ok else None,
            })
        return out

    def test_no_rain_no_segment(self):
        self.assertEqual(core.segment_day(self._hours()), [])

    def test_consecutive_rainy_hours_merge(self):
        segs = core.segment_day(self._hours(rainy={10, 11, 12}))
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0]["start_hour"], DAY1 + 10 * 3600)
        self.assertEqual(segs[0]["end_hour"], DAY1 + 12 * 3600)
        self.assertEqual(segs[0]["hours"], 3)
        self.assertEqual(segs[0]["total_pulses"], 360)
        self.assertEqual(segs[0]["rainfall_mm"], 72.0)

    def test_zero_rain_hour_breaks(self):
        segs = core.segment_day(self._hours(rainy={10, 12}))
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0]["hours"], 1)
        self.assertEqual(segs[1]["start_hour"], DAY1 + 12 * 3600)

    def test_incomplete_hour_breaks(self):
        segs = core.segment_day(self._hours(rainy={10, 11, 12}, incomplete={11}))
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0]["end_hour"], DAY1 + 10 * 3600)
        self.assertEqual(segs[1]["start_hour"], DAY1 + 12 * 3600)

    def test_full_day_single_segment(self):
        segs = core.segment_day(self._hours(rainy=set(range(24))))
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0]["hours"], 24)

    def test_boundary_hours(self):
        segs = core.segment_day(self._hours(rainy={0, 23}))
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0]["start_hour"], DAY1)
        self.assertEqual(segs[1]["end_hour"], DAY1 + 23 * 3600)


class CoreAffectedDaysTests(unittest.TestCase):
    def test_plain_minute_one_day(self):
        self.assertEqual(core.affected_days_for_minute(DAY1 + 10 * 3600 + 30 * 60), {DAY1})

    def test_on_hour_still_one_day(self):
        # 整点读数影响前后两小时，但同属一日
        self.assertEqual(core.affected_days_for_minute(DAY1 + 10 * 3600), {DAY1})

    def test_midnight_touches_two_days(self):
        # 午夜整点：既是前一日 23 时的末点，又是当日 00 时的起点
        self.assertEqual(core.affected_days_for_minute(DAY2), {DAY1, DAY2})

    def test_parse_date(self):
        self.assertEqual(core.parse_date("2026-11-17"), DAY1)
        with self.assertRaises(ValueError):
            core.parse_date("2026-13-01")
        with self.assertRaises(ValueError):
            core.parse_date("2026-11-17T00:00Z")
        with self.assertRaises(ValueError):
            core.parse_date("")


class CatalogAppTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "app.db")
        self.app = create_app(self.path)
        self.client = self.app.test_client()

    def _upload(self, text):
        resp = self.client.post("/api/upload", data={"csv_text": text})
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        return resp.get_json()

    def _catalog(self, station="S1", date="2026-11-17"):
        resp = self.client.get(f"/api/processes?station={station}&date={date}")
        self.assertEqual(resp.status_code, 200)
        return resp.get_json()

    def _process(self, code):
        resp = self.client.get(f"/api/process/{code}")
        self.assertEqual(resp.status_code, 200)
        return resp.get_json()

    # ---------- 基本生成与编号沿用 ----------

    def test_catalog_created_with_id_start_end_rainfall(self):
        rep = self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        self.assertEqual(len(rep["catalog_changes"]), 1)
        ch = rep["catalog_changes"][0]
        self.assertEqual((ch["station"], ch["date"]), ("S1", "2026-11-17"))
        self.assertEqual(ch["kept"], [])
        self.assertEqual(ch["superseded"], [])
        self.assertEqual(len(ch["created"]), 1)
        p = ch["created"][0]
        self.assertEqual(p["code"], "S1-20261117-1")
        self.assertEqual(p["start"], "2026-11-17T10:00:00Z")
        self.assertEqual(p["end"], "2026-11-17T13:00:00Z")
        self.assertEqual(p["hours"], 3)
        self.assertEqual(p["rainfall_mm"], 72.0)  # 3 小时 × 120 脉冲 × 0.2

        cat = self._catalog()
        self.assertEqual([p["code"] for p in cat["processes"]], ["S1-20261117-1"])
        # 逐小时：10~12 时归属该过程，雨量 24.0
        by_hour = {h["hour_start"]: h for h in cat["hours"]}
        self.assertEqual(by_hour["2026-11-17T10:00:00Z"]["process"], "S1-20261117-1")
        self.assertEqual(by_hour["2026-11-17T10:00:00Z"]["rainfall_mm"], 24.0)
        self.assertIsNone(by_hour["2026-11-17T09:00:00Z"]["process"])

    def test_unchanged_process_keeps_id(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        # 第二批：补全天备计零雨量读数（新键被接受，触发重算，但分段不变）
        rep = self._upload(day_csv("S1", DAY1, set(), device="backup"))
        ch = rep["catalog_changes"][0]
        self.assertEqual(ch["kept"], ["S1-20261117-1"])
        self.assertEqual(ch["created"], [])
        self.assertEqual(ch["superseded"], [])
        cat = self._catalog()
        self.assertEqual([p["code"] for p in cat["processes"]], ["S1-20261117-1"])
        self.assertEqual(cat["processes"][0]["status"], "active")

    def test_unrelated_day_does_not_touch_catalog(self):
        self._upload(day_csv("S1", DAY1, {10}))
        rep = self._upload(day_csv("S1", DAY2, {5, 6}))
        days = {(c["station"], c["date"]) for c in rep["catalog_changes"]}
        self.assertEqual(days, {("S1", "2026-11-18")})
        self.assertEqual([p["code"] for p in self._catalog()["processes"]],
                         ["S1-20261117-1"])

    # ---------- 变化与替代关联 ----------

    def test_extend_replaces_and_links_by_shared_hours(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        rep = self._upload(day_csv("S1", DAY1, {10, 11, 12, 13}, rev=2))
        ch = rep["catalog_changes"][0]
        self.assertEqual(ch["kept"], [])
        self.assertEqual([p["code"] for p in ch["created"]], ["S1-20261117-2"])
        sup = ch["superseded"][0]
        self.assertEqual(sup["code"], "S1-20261117-1")
        self.assertEqual(sup["rainfall_mm"], 72.0)  # 旧编号保留原结果
        self.assertEqual(sup["replacements"],
                         [{"code": "S1-20261117-2", "shared_hours": 3}])

        old = self._process("S1-20261117-1")
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["rainfall_mm"], 72.0)
        self.assertEqual(old["start"], "2026-11-17T10:00:00Z")
        self.assertEqual(old["end"], "2026-11-17T13:00:00Z")
        self.assertEqual([r["code"] for r in old["replacements"]], ["S1-20261117-2"])

        new = self._process("S1-20261117-2")
        self.assertEqual(new["status"], "active")
        self.assertEqual(new["rainfall_mm"], 96.0)
        self.assertEqual([p["code"] for p in new["predecessors"]], ["S1-20261117-1"])

        cat = self._catalog()
        self.assertEqual([p["code"] for p in cat["processes"]], ["S1-20261117-2"])

    def test_split_links_to_both_parts(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        rep = self._upload(day_csv("S1", DAY1, {10, 12}, rev=2))  # 11 时归零 -> 拆分
        ch = rep["catalog_changes"][0]
        self.assertEqual([p["code"] for p in ch["created"]],
                         ["S1-20261117-2", "S1-20261117-3"])
        sup = ch["superseded"][0]
        self.assertEqual(sup["code"], "S1-20261117-1")
        shared = {r["code"]: r["shared_hours"] for r in sup["replacements"]}
        self.assertEqual(shared, {"S1-20261117-2": 1, "S1-20261117-3": 1})
        cat = self._catalog()
        self.assertEqual([p["code"] for p in cat["processes"]],
                         ["S1-20261117-2", "S1-20261117-3"])

    def test_merge_links_from_all_old_parts(self):
        self._upload(day_csv("S1", DAY1, {10, 12}))            # 两段：10-11、12-13
        rep = self._upload(day_csv("S1", DAY1, {10, 11, 12}, rev=2))  # 合并为 10-13
        ch = rep["catalog_changes"][0]
        self.assertEqual([p["code"] for p in ch["created"]], ["S1-20261117-3"])
        self.assertEqual({s["code"] for s in ch["superseded"]},
                         {"S1-20261117-1", "S1-20261117-2"})
        for s in ch["superseded"]:
            self.assertEqual(s["replacements"],
                             [{"code": "S1-20261117-3", "shared_hours": 1}])
        new = self._process("S1-20261117-3")
        self.assertEqual({p["code"] for p in new["predecessors"]},
                         {"S1-20261117-1", "S1-20261117-2"})

    def test_disappeared_process_has_no_replacement(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        rep = self._upload(day_csv("S1", DAY1, set(), rev=2))  # 全部归零 -> 消失
        ch = rep["catalog_changes"][0]
        self.assertEqual(ch["created"], [])
        sup = ch["superseded"][0]
        self.assertEqual(sup["code"], "S1-20261117-1")
        self.assertEqual(sup["replacements"], [])
        old = self._process("S1-20261117-1")
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["replacements"], [])
        self.assertEqual(self._catalog()["processes"], [])
        # 网页可查“已消失”
        html = self.client.get("/process/S1-20261117-1").get_data(as_text=True)
        self.assertIn("已消失", html)
        self.assertIn("72.0", html)  # 原结果仍展示

    def test_incomplete_hour_breaks_segment(self):
        lines = day_csv("S1", DAY1, {10, 11, 12}).splitlines()
        drop = core.iso_utc(DAY1 + 11 * 3600 + 30 * 60)  # 删掉 11:30 -> 11 时不可供数
        lines = [l for l in lines if f",{drop}," not in l]
        self._upload("\n".join(lines))
        cat = self._catalog()
        self.assertEqual([p["code"] for p in cat["processes"]],
                         ["S1-20261117-1", "S1-20261117-2"])
        self.assertEqual(cat["processes"][0]["start"], "2026-11-17T10:00:00Z")
        self.assertEqual(cat["processes"][0]["end"], "2026-11-17T11:00:00Z")
        self.assertEqual(cat["processes"][1]["start"], "2026-11-17T12:00:00Z")
        by_hour = {h["hour_start"]: h for h in cat["hours"]}
        self.assertEqual(by_hour["2026-11-17T11:00:00Z"]["status"], "incomplete")
        self.assertIsNone(by_hour["2026-11-17T11:00:00Z"]["process"])

    def test_two_processes_only_changed_one_is_replaced(self):
        self._upload(day_csv("S1", DAY1, {2, 3, 10, 11, 12}))
        rep = self._upload(day_csv("S1", DAY1, {2, 3, 10, 11}, rev=2))  # 仅后段缩短
        ch = rep["catalog_changes"][0]
        self.assertEqual(ch["kept"], ["S1-20261117-1"])          # 前段沿用
        self.assertEqual([s["code"] for s in ch["superseded"]], ["S1-20261117-2"])
        self.assertEqual([p["code"] for p in ch["created"]], ["S1-20261117-3"])
        cat = self._catalog()
        self.assertEqual([p["code"] for p in cat["processes"]],
                         ["S1-20261117-1", "S1-20261117-3"])

    # ---------- 整点修订影响相邻两日 ----------

    def test_midnight_revision_updates_two_days_in_one_batch(self):
        text = span_csv("S2", DAY1, {DAY1: {23}, DAY2: {0}}, per_min=10)
        rep1 = self._upload(text)
        changes = {(c["station"], c["date"]): c for c in rep1["catalog_changes"]}
        self.assertEqual(set(changes), {("S2", "2026-11-17"), ("S2", "2026-11-18")})
        code_d1 = changes[("S2", "2026-11-17")]["created"][0]["code"]  # 23:00~24:00
        code_d2 = changes[("S2", "2026-11-18")]["created"][0]["code"]  # 00:00~01:00
        self.assertEqual(changes[("S2", "2026-11-17")]["created"][0]["rainfall_mm"], 120.0)
        self.assertEqual(changes[("S2", "2026-11-18")]["created"][0]["rainfall_mm"], 120.0)

        # 只修订午夜整点读数（1600 -> 1595，修订号 2）：前一日 23 时 -5 脉冲，当日 00 时 +5
        rep2 = self._upload(f"S2,primary,{core.iso_utc(DAY2)},1595,2")
        self.assertEqual(rep2["accepted_count"], 1)
        changes2 = {(c["station"], c["date"]): c for c in rep2["catalog_changes"]}
        # 同一批读数同时更新相邻两日目录
        self.assertEqual(set(changes2), {("S2", "2026-11-17"), ("S2", "2026-11-18")})
        d1, d2 = changes2[("S2", "2026-11-17")], changes2[("S2", "2026-11-18")]
        self.assertEqual([s["code"] for s in d1["superseded"]], [code_d1])
        self.assertEqual(d1["created"][0]["rainfall_mm"], 119.0)   # 595 脉冲
        self.assertEqual([s["code"] for s in d2["superseded"]], [code_d2])
        self.assertEqual(d2["created"][0]["rainfall_mm"], 121.0)   # 605 脉冲
        # 两日目录分别只有新编号
        self.assertEqual([p["code"] for p in self._catalog("S2", "2026-11-17")["processes"]],
                         [d1["created"][0]["code"]])
        self.assertEqual([p["code"] for p in self._catalog("S2", "2026-11-18")["processes"]],
                         [d2["created"][0]["code"]])

    # ---------- 原子性：目录更新失败不留半批 ----------

    def test_catalog_failure_rolls_back_whole_batch(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        before_cat = self._catalog()
        before_hour = self.client.get(
            "/api/hourly?station=S1&hour=2026-11-17T10:00Z").get_json()
        before_batches = self.client.get("/api/batches").get_json()

        with mock.patch.object(db, "reconcile_catalog",
                               side_effect=RuntimeError("catalog boom")):
            resp = self.client.post(
                "/api/upload", data={"csv_text": day_csv("S1", DAY1, {10, 11, 12, 13}, rev=2)})
        self.assertEqual(resp.status_code, 500)

        # 目录、小时雨量、批次列表全部保持旧状态：没有半批新结果
        self.assertEqual(self._catalog(), before_cat)
        self.assertEqual(self.client.get(
            "/api/hourly?station=S1&hour=2026-11-17T10:00Z").get_json(), before_hour)
        self.assertEqual(self.client.get("/api/batches").get_json(), before_batches)

    def test_db_level_catalog_failure_rolls_back(self):
        path = os.path.join(self.dir, "dblevel.db")
        conn = db.connect(path)
        db.init_db(conn)
        try:
            rows = [(i + 1, "raw", core.Reading("S1", "primary", DAY1 + 10 * 3600 + i * 60,
                                                1000 + i, 1)) for i in range(61)]
            with mock.patch.object(db, "reconcile_catalog",
                                   side_effect=RuntimeError("boom")):
                with self.assertRaises(RuntimeError):
                    db.apply_batch(conn, rows, [], "x.csv")
            for table in ("readings", "batches", "batch_rows", "processes", "process_links"):
                self.assertEqual(
                    conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0, table)
        finally:
            conn.close()

    # ---------- 网页 / API 一致性与重启持久化 ----------

    def test_web_and_api_consistent_after_revision(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        html = self.client.get("/day?station=S1&date=2026-11-17").get_data(as_text=True)
        self.assertIn("S1-20261117-1", html)
        self.assertIn("72.0", html)

        self._upload(day_csv("S1", DAY1, {10, 11, 12, 13}, rev=2))
        html = self.client.get("/day?station=S1&date=2026-11-17").get_data(as_text=True)
        cat = self._catalog()
        # 网页与 API 同时只展示新状态
        self.assertIn("S1-20261117-2", html)
        self.assertIn("96.0", html)
        self.assertEqual([p["code"] for p in cat["processes"]], ["S1-20261117-2"])
        # 过程页：旧编号展示原结果与替代关联
        old_html = self.client.get("/process/S1-20261117-1").get_data(as_text=True)
        self.assertIn("已被替代", old_html)
        self.assertIn("S1-20261117-2", old_html)
        # 批次页也展示目录变更
        bh = self.client.get("/batches/2").get_data(as_text=True)
        self.assertIn("S1-20261117-2", bh)
        self.assertIn("S1-20261117-1", bh)

    def test_upload_html_report_shows_catalog_changes(self):
        resp = self.client.post("/upload", data={"csv_text": day_csv("S1", DAY1, {10})})
        html = resp.get_data(as_text=True)
        self.assertIn("S1-20261117-1", html)
        self.assertIn("过程目录变更", html)

    def test_restart_keeps_old_codes_queryable(self):
        self._upload(day_csv("S1", DAY1, {10, 11, 12}))
        self._upload(day_csv("S1", DAY1, {10, 11, 12, 13}, rev=2))
        # 模拟重启：同一数据库文件新建应用实例
        client2 = create_app(self.path).test_client()
        old = client2.get("/api/process/S1-20261117-1").get_json()
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["rainfall_mm"], 72.0)
        self.assertEqual([r["code"] for r in old["replacements"]], ["S1-20261117-2"])
        cat = client2.get("/api/processes?station=S1&date=2026-11-17").get_json()
        self.assertEqual([p["code"] for p in cat["processes"]], ["S1-20261117-2"])
        html = client2.get("/process/S1-20261117-1").get_data(as_text=True)
        self.assertIn("已被替代", html)

    def test_api_param_validation(self):
        self.assertEqual(self.client.get("/api/processes?date=2026-11-17").status_code, 400)
        self.assertEqual(self.client.get("/api/processes?station=S1&date=bad").status_code, 400)
        self.assertEqual(self.client.get("/api/process/NOPE-1").status_code, 404)
        # 网页参数错误：闪消息并回首页
        resp = self.client.get("/day?station=S1&date=bad")
        self.assertEqual(resp.status_code, 302)
        resp = self.client.get("/process/NOPE-1")
        self.assertEqual(resp.status_code, 302)


if __name__ == "__main__":
    unittest.main()
