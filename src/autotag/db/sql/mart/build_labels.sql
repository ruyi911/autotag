CREATE OR REPLACE TABLE mart.user_state_engine AS
WITH cfg AS (
  SELECT
    COALESCE(MAX(CASE WHEN key = 'stable_b_gap_threshold' THEN value END), 2) AS stable_b,
    COALESCE(MAX(CASE WHEN key = 'stable_r_gap_threshold' THEN value END), 5) AS stable_r,
    COALESCE(MAX(CASE WHEN key = 'lost_b_gap_threshold' THEN value END), 5) AS lost_b,
    COALESCE(MAX(CASE WHEN key = 'lost_r_gap_threshold' THEN value END), 10) AS lost_r,
    COALESCE(MAX(CASE WHEN key = 'bet_drop_threshold' THEN value END), 0.6) AS bet_drop_th,
    COALESCE(MAX(CASE WHEN key = 'rech_drop_threshold' THEN value END), 0.6) AS rech_drop_th,
    COALESCE(MAX(CASE WHEN key = 'wd_rate_long_threshold' THEN value END), 0.8) AS wd_long_th,
    COALESCE(MAX(CASE WHEN key = 'pay_friction_fail_rate_threshold' THEN value END), 0.5) AS pay_fail_th
  FROM ops_config.thresholds
),
base AS (
  SELECT
    a.user_id,
    a.as_of_date,
    a.last_bet_day,
    a.last_recharge_day,
    a.last_withdraw_day,
    CASE WHEN a.last_bet_day IS NULL THEN NULL ELSE DATE_DIFF('day', a.last_bet_day, a.as_of_date) END AS b_gap,
    CASE WHEN a.last_recharge_day IS NULL THEN NULL ELSE DATE_DIFF('day', a.last_recharge_day, a.as_of_date) END AS r_gap,
    CASE WHEN u.register_time IS NULL THEN NULL ELSE DATE_DIFF('day', DATE(u.register_time), a.as_of_date) END AS reg_gap,
    COALESCE(cu.total_recharge_count, 0) AS total_recharge_count,
    COALESCE(cu.total_recharge_amount, 0) AS total_recharge_amount,
    m.bet_drop,
    m.rech_drop,
    m.wd_rate_short,
    m.wd_rate_long,
    m.bonus_rate_rech,
    m.bonus_rate_bet,
    m.p_short,
    m.p_long,
    m.pay_fail_rate_short,
    m.pay_fail_rate_long
  FROM mart.user_last_activity a
  LEFT JOIN mart.user_window_metrics m ON a.user_id = m.user_id
  LEFT JOIN mart.fact_user u ON a.user_id = u.user_id
  LEFT JOIN mart.user_cumulative cu ON a.user_id = cu.user_id
),
calc AS (
  SELECT
    b.*,
    c.*,
    b.total_recharge_amount / CAST(GREATEST(COALESCE(b.reg_gap, 0) + 1, 1) AS DOUBLE) AS avg_recharge_per_day,
    CASE
      WHEN b.wd_rate_long IS NOT NULL AND b.wd_rate_long >= c.wd_long_th THEN '资金回收型'
      ELSE '正常循环'
    END AS funding_type,
    CASE
      WHEN (b.bet_drop IS NOT NULL AND b.bet_drop < c.bet_drop_th)
       AND (b.rech_drop IS NOT NULL AND b.rech_drop < c.rech_drop_th) THEN '双衰减'
      WHEN (b.bet_drop IS NOT NULL AND b.bet_drop < c.bet_drop_th) THEN '投注衰减'
      WHEN (b.rech_drop IS NOT NULL AND b.rech_drop < c.rech_drop_th) THEN '充值衰减'
      ELSE '稳定'
    END AS reason_code,
    CASE
      WHEN b.pay_fail_rate_short IS NOT NULL AND b.pay_fail_rate_short >= c.pay_fail_th THEN '支付摩擦-急'
      WHEN b.pay_fail_rate_long IS NOT NULL AND b.pay_fail_rate_long >= c.pay_fail_th THEN '支付摩擦-持续'
      ELSE '支付正常'
    END AS pay_friction_tag
  FROM base b
  CROSS JOIN cfg c
),
state_base AS (
  SELECT
    user_id,
    as_of_date,
    last_bet_day,
    last_recharge_day,
    last_withdraw_day,
    b_gap,
    r_gap,
    CASE
      WHEN last_bet_day IS NULL AND last_recharge_day IS NULL THEN '未激活'
      WHEN last_bet_day IS NULL AND last_recharge_day IS NOT NULL THEN '只充不玩'
      WHEN last_bet_day IS NOT NULL AND last_recharge_day IS NULL THEN '只玩不充'
      WHEN b_gap < stable_b AND r_gap < stable_r THEN '健康稳定'
      WHEN b_gap >= lost_b AND r_gap >= lost_r THEN '已流失'
      WHEN b_gap >= stable_b AND r_gap >= stable_r THEN '高危流失'
      ELSE '变慢预警'
    END AS state_4,
    CASE
      WHEN total_recharge_count = 0 AND reg_gap IS NOT NULL AND reg_gap < 3 THEN '新注册未充值'
      WHEN total_recharge_count = 0 AND (reg_gap IS NULL OR reg_gap >= 3) THEN '注册未首充'
      WHEN total_recharge_count = 1 AND r_gap IS NOT NULL AND r_gap >= 7 THEN '首充未复充'
      WHEN avg_recharge_per_day >= 400 THEN '高价值用户'
      WHEN (r_gap IS NOT NULL AND r_gap <= 7) OR (b_gap IS NOT NULL AND b_gap <= 7) THEN '活跃用户'
      WHEN COALESCE(r_gap, 99999) >= 7 AND COALESCE(b_gap, 99999) >= 7
           AND (COALESCE(r_gap, 99999) < 15 OR COALESCE(b_gap, 99999) < 15) THEN '沉默用户'
      WHEN COALESCE(r_gap, 99999) >= 15 AND COALESCE(b_gap, 99999) >= 15 THEN '流失用户'
      ELSE NULL
    END AS lifecycle_tag,
    CASE
      WHEN last_bet_day IS NULL OR last_recharge_day IS NULL THEN NULL
      WHEN b_gap < stable_b AND r_gap >= stable_r THEN '还玩但不充'
      WHEN b_gap >= stable_b AND r_gap < stable_r THEN '还充但不玩'
      ELSE NULL
    END AS sub_state,
    bet_drop,
    rech_drop,
    reason_code,
    wd_rate_short,
    wd_rate_long,
    funding_type,
    bonus_rate_rech,
    bonus_rate_bet,
    p_short,
    p_long,
    pay_fail_rate_short,
    pay_fail_rate_long,
    pay_friction_tag
  FROM calc
)
SELECT
  *,
  CASE
    WHEN p_short IS NULL THEN NULL
    WHEN p_short < 0 AND (reason_code <> '稳定' OR state_4 IN ('变慢预警', '高危流失')) THEN '亏损型衰减'
    WHEN p_short > 0 AND funding_type = '资金回收型' THEN '盈利型退出/回收'
    WHEN p_long IS NOT NULL AND ((p_short >= 0 AND p_long < 0) OR (p_short < 0 AND p_long >= 0)) THEN '高波动型'
    ELSE '均衡型自然衰减'
  END AS profit_reason_tag
FROM state_base;
