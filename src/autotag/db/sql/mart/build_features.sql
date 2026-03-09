CREATE OR REPLACE TABLE mart.user_last_activity AS
WITH run AS (
  SELECT ?::DATE AS as_of_date
),
u AS (
  SELECT user_id FROM mart.fact_user
),
lb AS (
  SELECT user_id, MAX(biz_date) AS last_bet_day
  FROM mart.fact_bet
  WHERE biz_date <= (SELECT as_of_date FROM run)
  GROUP BY 1
),
lr_succ AS (
  SELECT user_id, MAX(biz_date) AS last_recharge_day
  FROM mart.fact_recharge
  WHERE status_raw = '充值成功' AND biz_date <= (SELECT as_of_date FROM run)
  GROUP BY 1
),
lr_all AS (
  SELECT user_id, MAX(biz_date) AS last_recharge_attempt_day
  FROM mart.fact_recharge
  WHERE biz_date <= (SELECT as_of_date FROM run)
  GROUP BY 1
),
lw_succ AS (
  SELECT user_id, MAX(biz_date) AS last_withdraw_day
  FROM mart.fact_withdraw
  WHERE status_raw = '三方付款成功' AND biz_date <= (SELECT as_of_date FROM run)
  GROUP BY 1
)
SELECT
  u.user_id,
  run.as_of_date,
  lb.last_bet_day,
  lr_succ.last_recharge_day,
  lr_all.last_recharge_attempt_day,
  lw_succ.last_withdraw_day
FROM u
CROSS JOIN run
LEFT JOIN lb ON u.user_id = lb.user_id
LEFT JOIN lr_succ ON u.user_id = lr_succ.user_id
LEFT JOIN lr_all ON u.user_id = lr_all.user_id
LEFT JOIN lw_succ ON u.user_id = lw_succ.user_id;

CREATE OR REPLACE TABLE mart.user_window_metrics AS
WITH cfg AS (
  SELECT
    COALESCE(MAX(CASE WHEN key = 'bet_drop_short_days' THEN value END), 7)::INT AS bet_s,
    COALESCE(MAX(CASE WHEN key = 'bet_drop_long_days'  THEN value END), 30)::INT AS bet_l,
    COALESCE(MAX(CASE WHEN key = 'wd_short_days'       THEN value END), 7)::INT AS wd_s,
    COALESCE(MAX(CASE WHEN key = 'wd_long_days'        THEN value END), 30)::INT AS wd_l,
    COALESCE(MAX(CASE WHEN key = 'p_short_days'        THEN value END), 7)::INT AS p_s,
    COALESCE(MAX(CASE WHEN key = 'p_long_days'         THEN value END), 30)::INT AS p_l,
    COALESCE(MAX(CASE WHEN key = 'pay_friction_short_days' THEN value END), 1)::INT AS pf_s,
    COALESCE(MAX(CASE WHEN key = 'pay_friction_long_days'  THEN value END), 3)::INT AS pf_l
  FROM ops_config.thresholds
),
run AS (
  SELECT ?::DATE AS as_of_date
),
base AS (
  SELECT
    d.user_id,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_s - 1) AND run.as_of_date THEN d.bet_cnt ELSE 0 END) AS bet_cnt_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_s - 1) AND run.as_of_date THEN d.bet_amt ELSE 0 END) AS bet_amt_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_s - 1) AND run.as_of_date THEN d.recharge_cnt_success ELSE 0 END) AS rech_cnt_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_s - 1) AND run.as_of_date THEN d.recharge_amt_success ELSE 0 END) AS rech_amt_s,

    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.bet_cnt ELSE 0 END) AS bet_cnt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.bet_amt ELSE 0 END) AS bet_amt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.recharge_cnt_success ELSE 0 END) AS rech_cnt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.recharge_amt_success ELSE 0 END) AS rech_amt_l,

    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.wd_s - 1) AND run.as_of_date THEN d.withdraw_amt_success ELSE 0 END) AS wd_amt_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.wd_s - 1) AND run.as_of_date THEN d.recharge_amt_success ELSE 0 END) AS wd_rech_amt_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.wd_l - 1) AND run.as_of_date THEN d.withdraw_amt_success ELSE 0 END) AS wd_amt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.wd_l - 1) AND run.as_of_date THEN d.recharge_amt_success ELSE 0 END) AS wd_rech_amt_l,

    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.p_s - 1) AND run.as_of_date THEN d.profit ELSE 0 END) AS p_s_val,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.p_l - 1) AND run.as_of_date THEN d.profit ELSE 0 END) AS p_l_val,

    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.bonus_amt ELSE 0 END) AS bonus_amt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.bet_amt ELSE 0 END) AS bonus_bet_amt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.bet_l - 1) AND run.as_of_date THEN d.recharge_amt_success ELSE 0 END) AS bonus_rech_amt_l,

    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.pf_s - 1) AND run.as_of_date THEN d.recharge_cnt_total ELSE 0 END) AS pay_attempt_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.pf_s - 1) AND run.as_of_date THEN d.recharge_cnt_fail ELSE 0 END) AS pay_fail_s,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.pf_l - 1) AND run.as_of_date THEN d.recharge_cnt_total ELSE 0 END) AS pay_attempt_l,
    SUM(CASE WHEN d.biz_date BETWEEN run.as_of_date - (c.pf_l - 1) AND run.as_of_date THEN d.recharge_cnt_fail ELSE 0 END) AS pay_fail_l,

    ANY_VALUE(c.bet_s) AS cfg_bet_s,
    ANY_VALUE(c.bet_l) AS cfg_bet_l
  FROM mart.user_profile_daily d
  CROSS JOIN cfg c
  CROSS JOIN run
  GROUP BY 1
),
final AS (
  SELECT
    user_id,
    CASE
      WHEN cfg_bet_l = 0 OR cfg_bet_s = 0 OR bet_amt_l = 0 THEN NULL
      ELSE (bet_amt_s / CAST(cfg_bet_s AS DOUBLE)) / (bet_amt_l / CAST(cfg_bet_l AS DOUBLE))
    END AS bet_drop,
    CASE
      WHEN cfg_bet_l = 0 OR cfg_bet_s = 0 OR rech_amt_l = 0 THEN NULL
      ELSE (rech_amt_s / CAST(cfg_bet_s AS DOUBLE)) / (rech_amt_l / CAST(cfg_bet_l AS DOUBLE))
    END AS rech_drop,

    CASE WHEN wd_rech_amt_s = 0 THEN NULL ELSE wd_amt_s / wd_rech_amt_s END AS wd_rate_short,
    CASE WHEN wd_rech_amt_l = 0 THEN NULL ELSE wd_amt_l / wd_rech_amt_l END AS wd_rate_long,
    CASE WHEN bonus_rech_amt_l = 0 THEN NULL ELSE bonus_amt_l / bonus_rech_amt_l END AS bonus_rate_rech,
    CASE WHEN bonus_bet_amt_l = 0 THEN NULL ELSE bonus_amt_l / bonus_bet_amt_l END AS bonus_rate_bet,

    p_s_val AS p_short,
    p_l_val AS p_long,
    CASE WHEN pay_attempt_s = 0 THEN NULL ELSE pay_fail_s * 1.0 / pay_attempt_s END AS pay_fail_rate_short,
    CASE WHEN pay_attempt_l = 0 THEN NULL ELSE pay_fail_l * 1.0 / pay_attempt_l END AS pay_fail_rate_long,

    bet_cnt_s, bet_cnt_l, rech_cnt_s, rech_cnt_l,
    wd_amt_s, wd_rech_amt_s, wd_amt_l, wd_rech_amt_l, bonus_amt_l
  FROM base
)
SELECT * FROM final;


CREATE OR REPLACE TABLE mart.user_cumulative AS
WITH run AS (
  SELECT ?::DATE AS as_of_date
),
recharge_cumulative AS (
  SELECT
    user_id,
    SUM(CASE WHEN status_raw = '充值成功' THEN 1 ELSE 0 END) AS total_recharge_count,
    SUM(CASE WHEN status_raw = '充值成功' THEN COALESCE(pay_amt_raw, 0) ELSE 0 END) AS total_recharge_amount
  FROM mart.fact_recharge
  WHERE biz_date <= (SELECT as_of_date FROM run)
  GROUP BY 1
)
SELECT
  user_id,
  COALESCE(total_recharge_count, 0) AS total_recharge_count,
  COALESCE(total_recharge_amount, 0) AS total_recharge_amount
FROM recharge_cumulative;
