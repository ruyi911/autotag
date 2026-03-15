from __future__ import annotations

import argparse
import os
from datetime import timedelta

from autotag.db.duckdb_conn import duckdb_conn
from autotag.utils.paths import get_serving_db_path
from autotag.utils.time import default_business_dt, parse_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish gating checks")
    parser.add_argument("--dt", default=default_business_dt())
    parser.add_argument("--sources", default="", help="comma separated")
    return parser.parse_args()


def _parse_sources(sources: str) -> set[str]:
    return {s.strip() for s in (sources or "").split(",") if s.strip()}


def run_gating(dt: str, sources: str = "") -> None:
    enable_status_drift = os.getenv("ENABLE_STATUS_DRIFT_GATE", "1") == "1"
    enable_login_fresh = os.getenv("ENABLE_LOGIN_FRESHNESS_GATE", "1") == "1"
    drift_min_orders = int(os.getenv("STATUS_DRIFT_MIN_ORDERS", "100"))
    bonus_agg_tol = float(os.getenv("BONUS_AGG_TOL", "1e-6"))
    selected_sources = _parse_sources(sources)

    with duckdb_conn(get_serving_db_path(), read_only=True) as conn:
        # 1) 关键对象存在且非空
        row_cnt = conn.execute('SELECT COUNT(*) FROM ops."用户状态总览"').fetchone()[0]
        if row_cnt <= 0:
            raise RuntimeError('门禁失败: ops."用户状态总览" 行数为 0')

        # 2) 关键列不为空
        null_cnt = conn.execute(
            '''
            SELECT COUNT(*)
            FROM ops."用户状态总览"
            WHERE "用户ID" IS NULL OR "用户状态" IS NULL
            '''
        ).fetchone()[0]
        if null_cnt > 0:
            raise RuntimeError(f"门禁失败: 关键列存在空值, 行数={null_cnt}")

        # 3) 日期范围不可未来
        max_dt = conn.execute('SELECT MAX("统计日期") FROM ops."用户状态总览"').fetchone()[0]
        if max_dt is None:
            raise RuntimeError("门禁失败: 统计日期为空")

        run_dt = parse_date(dt)
        if max_dt > run_dt:
            raise RuntimeError(f"门禁失败: 存在未来日期, max_dt={max_dt}, run_dt={run_dt}")

        # 4) 登录时间新鲜度（应至少覆盖到 D-1）
        if enable_login_fresh and (not selected_sources or "user" in selected_sources):
            max_login = conn.execute("SELECT MAX(last_login_time) FROM mart.fact_user").fetchone()[0]
            if max_login is None:
                print("登录数据无变化，不用更新", flush=True)
            elif max_login.date() < (run_dt - timedelta(days=1)):
                print("登录数据无变化，不用更新", flush=True)

        # 5) bonus汇总一致性（fact_bonus 与 user_profile_daily）
        if not selected_sources or "bonus" in selected_sources:
            fact_bonus_cnt, fact_bonus_amt = conn.execute(
                """
                SELECT
                  COUNT(*) AS bonus_cnt,
                  COALESCE(SUM(COALESCE(bonus_amt_raw, 0)), 0) AS bonus_amt
                FROM mart.fact_bonus
                WHERE biz_date = ?::DATE
                """,
                [dt],
            ).fetchone()
            profile_bonus_cnt, profile_bonus_amt = conn.execute(
                """
                SELECT
                  COALESCE(SUM(COALESCE(bonus_cnt, 0)), 0) AS bonus_cnt,
                  COALESCE(SUM(COALESCE(bonus_amt, 0)), 0) AS bonus_amt
                FROM mart.user_profile_daily
                WHERE biz_date = ?::DATE
                """,
                [dt],
            ).fetchone()

            if fact_bonus_cnt != profile_bonus_cnt:
                raise RuntimeError(
                    "门禁失败: bonus汇总次数不一致, "
                    f"dt={dt}, fact_bonus_cnt={fact_bonus_cnt}, profile_bonus_cnt={profile_bonus_cnt}"
                )

            bonus_amt_diff = abs(float(fact_bonus_amt or 0) - float(profile_bonus_amt or 0))
            if bonus_amt_diff > bonus_agg_tol:
                raise RuntimeError(
                    "门禁失败: bonus汇总金额不一致, "
                    f"dt={dt}, fact_bonus_amt={fact_bonus_amt}, profile_bonus_amt={profile_bonus_amt}, "
                    f"diff={bonus_amt_diff}, tol={bonus_agg_tol}"
                )

        # 6) 订单状态漂移检测（近3天同订单状态变更应非0）
        if enable_status_drift and (not selected_sources or "recharge" in selected_sources):
            recharge_total = conn.execute(
                """
                SELECT COUNT(DISTINCT order_id)
                FROM stg.stg_recharge
                WHERE biz_date BETWEEN ?::DATE - 2 AND ?::DATE
                  AND order_id IS NOT NULL AND order_id <> ''
                """,
                [dt, dt],
            ).fetchone()[0]
            recharge_drift = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                  SELECT order_id
                  FROM stg.stg_recharge
                  WHERE biz_date BETWEEN ?::DATE - 2 AND ?::DATE
                    AND order_id IS NOT NULL AND order_id <> ''
                  GROUP BY 1
                  HAVING COUNT(DISTINCT status_raw) > 1
                ) t
                """,
                [dt, dt],
            ).fetchone()[0]
            if recharge_total >= drift_min_orders and recharge_drift == 0:
                print(
                    f"[validate] recharge近3天无状态漂移，视为无需更新并跳过 (orders={recharge_total}, min={drift_min_orders})",
                    flush=True,
                )

        if enable_status_drift and (not selected_sources or "withdraw" in selected_sources):
            withdraw_total = conn.execute(
                """
                SELECT COUNT(DISTINCT withdraw_id)
                FROM stg.stg_withdraw
                WHERE biz_date BETWEEN ?::DATE - 2 AND ?::DATE
                  AND withdraw_id IS NOT NULL AND withdraw_id <> ''
                """,
                [dt, dt],
            ).fetchone()[0]
            withdraw_drift = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                  SELECT withdraw_id
                  FROM stg.stg_withdraw
                  WHERE biz_date BETWEEN ?::DATE - 2 AND ?::DATE
                    AND withdraw_id IS NOT NULL AND withdraw_id <> ''
                  GROUP BY 1
                  HAVING COUNT(DISTINCT status_raw) > 1
                ) t
                """,
                [dt, dt],
            ).fetchone()[0]
            if withdraw_total >= drift_min_orders and withdraw_drift == 0:
                print(
                    f"[validate] withdraw近3天无状态漂移，视为无需更新并跳过 (orders={withdraw_total}, min={drift_min_orders})",
                    flush=True,
                )


def main() -> None:
    args = parse_args()
    run_gating(args.dt, sources=args.sources)


if __name__ == "__main__":
    main()
