CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.user_login_latest AS
SELECT
  user_id,
  MAX(last_login_time) AS last_login_time_latest
FROM stg.stg_user
WHERE user_id <> ''
GROUP BY 1;

CREATE OR REPLACE TABLE mart.fact_user AS
WITH base AS (
  SELECT * EXCLUDE (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COALESCE(last_login_time, register_time) DESC, ingested_at DESC) AS rn
    FROM stg.stg_user
  ) t
  WHERE rn = 1
)
SELECT
  b.* REPLACE (COALESCE(l.last_login_time_latest, b.last_login_time) AS last_login_time)
FROM base b
LEFT JOIN mart.user_login_latest l ON b.user_id = l.user_id;

CREATE OR REPLACE TABLE mart.fact_recharge AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(pay_time, created_time) DESC, ingested_at DESC) AS rn
  FROM stg.stg_recharge
  WHERE order_id IS NOT NULL AND order_id <> ''
) t
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.fact_bet AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY bet_id ORDER BY biz_date DESC, ingested_at DESC) AS rn
  FROM stg.stg_bet
  WHERE bet_id <> ''
) t
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.fact_withdraw AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY withdraw_id ORDER BY COALESCE(finish_time, submit_time, apply_time) DESC, ingested_at DESC) AS rn
  FROM stg.stg_withdraw
  WHERE withdraw_id <> ''
) t
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.fact_bonus AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id, claim_time, bonus_amt_raw ORDER BY ingested_at DESC) AS rn
  FROM stg.stg_bonus
  WHERE user_id <> ''
) t
WHERE rn = 1;

CREATE OR REPLACE TABLE mart.user_profile_daily AS
WITH keys AS (
  SELECT user_id, biz_date FROM mart.fact_recharge
  UNION
  SELECT user_id, biz_date FROM mart.fact_withdraw
  UNION
  SELECT user_id, biz_date FROM mart.fact_bet
  UNION
  SELECT user_id, biz_date FROM mart.fact_bonus
),
recharge_daily AS (
  SELECT
    user_id,
    biz_date,
    SUM(CASE WHEN status_raw = '充值成功' THEN COALESCE(pay_amt_raw, 0) ELSE 0 END) AS recharge_amt_success,
    SUM(CASE WHEN status_raw = '充值成功' THEN 1 ELSE 0 END) AS recharge_cnt_success,
    COUNT(*) AS recharge_cnt_total,
    SUM(CASE WHEN status_raw = '支付中' THEN 1 ELSE 0 END) AS recharge_cnt_fail
  FROM mart.fact_recharge
  GROUP BY 1,2
),
withdraw_daily AS (
  SELECT
    user_id,
    biz_date,
    SUM(CASE WHEN status_raw = '三方付款成功' THEN COALESCE(withdraw_amt_raw, 0) ELSE 0 END) AS withdraw_amt_success,
    COUNT(*) AS withdraw_cnt_total,
    SUM(CASE WHEN status_raw = '三方付款成功' THEN 1 ELSE 0 END) AS withdraw_cnt_success,
    SUM(CASE WHEN status_raw = '三方付款失败' THEN 1 ELSE 0 END) AS withdraw_cnt_fail,
    SUM(CASE WHEN status_raw = '审核拒绝' THEN 1 ELSE 0 END) AS withdraw_cnt_reject
  FROM mart.fact_withdraw
  GROUP BY 1,2
),
bet_daily AS (
  SELECT
    user_id,
    biz_date,
    COUNT(*) AS bet_cnt,
    SUM(COALESCE(bet_amt_raw, 0)) AS bet_amt,
    SUM(COALESCE(payout_amt_raw, 0)) AS payout,
    SUM(COALESCE(profit_amt_raw, 0)) AS profit
  FROM mart.fact_bet
  GROUP BY 1,2
),
bonus_daily AS (
  SELECT
    user_id,
    biz_date,
    COUNT(*) AS bonus_cnt,
    SUM(COALESCE(bonus_amt_raw, 0)) AS bonus_amt
  FROM mart.fact_bonus
  GROUP BY 1,2
)
SELECT
  k.user_id,
  k.biz_date,
  COALESCE(rd.recharge_amt_success, 0) AS recharge_amt_success,
  COALESCE(rd.recharge_cnt_success, 0) AS recharge_cnt_success,
  COALESCE(rd.recharge_cnt_total, 0) AS recharge_cnt_total,
  COALESCE(rd.recharge_cnt_fail, 0) AS recharge_cnt_fail,
  COALESCE(wd.withdraw_amt_success, 0) AS withdraw_amt_success,
  COALESCE(wd.withdraw_cnt_total, 0) AS withdraw_cnt_total,
  COALESCE(wd.withdraw_cnt_success, 0) AS withdraw_cnt_success,
  COALESCE(wd.withdraw_cnt_fail, 0) AS withdraw_cnt_fail,
  COALESCE(wd.withdraw_cnt_reject, 0) AS withdraw_cnt_reject,
  COALESCE(bd.bet_cnt, 0) AS bet_cnt,
  COALESCE(bd.bet_amt, 0) AS bet_amt,
  COALESCE(bd.payout, 0) AS payout,
  COALESCE(bd.profit, 0) AS profit,
  COALESCE(bod.bonus_cnt, 0) AS bonus_cnt,
  COALESCE(bod.bonus_amt, 0) AS bonus_amt
FROM keys k
LEFT JOIN recharge_daily rd ON k.user_id = rd.user_id AND k.biz_date = rd.biz_date
LEFT JOIN withdraw_daily wd ON k.user_id = wd.user_id AND k.biz_date = wd.biz_date
LEFT JOIN bet_daily bd ON k.user_id = bd.user_id AND k.biz_date = bd.biz_date
LEFT JOIN bonus_daily bod ON k.user_id = bod.user_id AND k.biz_date = bod.biz_date;
