CREATE SCHEMA IF NOT EXISTS stg;

CREATE OR REPLACE TABLE stg.stg_user AS
SELECT
  TRIM(BOTH '''' FROM "ID") AS user_id,
  "用户信息" AS user_name,
  "手机号" AS phone,
  "银行卡号" AS bank_card,
  TRIM(BOTH '''' FROM "上级ID") AS parent_user_id,
  TRY_CAST("VIP等级" AS INTEGER) AS vip_level,
  TRY_CAST("分销等级" AS INTEGER) AS distribution_level,
  "渠道" AS channel_raw,
  COALESCE(TRY_STRPTIME("注册时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("注册时间", '%Y/%m/%d %H:%M:%S')) AS register_time,
  "注册IP" AS register_ip,
  COALESCE(TRY_STRPTIME("最后登录时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("最后登录时间", '%Y/%m/%d %H:%M:%S')) AS last_login_time,
  COALESCE(TRY_STRPTIME("首充时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("首充时间", '%Y/%m/%d %H:%M:%S')) AS first_recharge_time,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("累计流水", '0')), ',', '', 'g') AS DOUBLE) AS turnover_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("累计充值", '0')), ',', '', 'g') AS DOUBLE) AS lifetime_recharge_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("累计提现", '0')), ',', '', 'g') AS DOUBLE) AS lifetime_withdraw_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("Cash余额", '0')), ',', '', 'g') AS DOUBLE) AS cash_balance_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("JCoin余额", '0')), ',', '', 'g') AS DOUBLE) AS jcoin_balance_raw,
  "账户状态" AS account_status_raw,
  DATE(COALESCE(TRY_STRPTIME("注册时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("注册时间", '%Y/%m/%d %H:%M:%S'))) AS biz_date,
  dt,
  source_file,
  file_hash,
  ingested_at
FROM raw.raw_user
WHERE TRIM(BOTH '''' FROM "ID") <> '';

CREATE OR REPLACE TABLE stg.stg_recharge AS
SELECT
  TRIM(BOTH '''' FROM "用户ID") AS user_id,
  "渠道来源" AS channel_source,
  "用户昵称" AS user_nickname,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("用户累充", '0')), ',', '', 'g') AS DOUBLE) AS user_lifetime_recharge,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("用户累提", '0')), ',', '', 'g') AS DOUBLE) AS user_lifetime_withdraw,
  "手机号" AS phone,
  COALESCE(TRY_STRPTIME("创建时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("创建时间", '%Y/%m/%d %H:%M:%S')) AS created_time,
  REGEXP_REPLACE(TRIM(BOTH '"' FROM TRIM(BOTH '''' FROM COALESCE("订单号", ''))), '^\s+', '') AS order_id,
  REGEXP_REPLACE(TRIM(BOTH '"' FROM TRIM(BOTH '''' FROM COALESCE("三方订单号", ''))), '^\s+', '') AS third_order_id,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("用户获得金额", '0')), ',', '', 'g') AS DOUBLE) AS gain_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("实际支付金额", '0')), ',', '', 'g') AS DOUBLE) AS pay_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("赠送金额", '0')), ',', '', 'g') AS DOUBLE) AS gift_amt_raw,
  "支付方式" AS pay_method,
  "订单状态" AS status_raw,
  COALESCE(TRY_STRPTIME("支付时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("支付时间", '%Y/%m/%d %H:%M:%S')) AS pay_time,
  "支付通道" AS pay_channel,
  "是否首充" AS is_first_recharge_raw,
  COALESCE(TRY_STRPTIME("注册时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("注册时间", '%Y/%m/%d %H:%M:%S')) AS register_time,
  DATE(COALESCE(TRY_STRPTIME("支付时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("支付时间", '%Y/%m/%d %H:%M:%S'))) AS biz_date,
  dt,
  source_file,
  file_hash,
  ingested_at
FROM raw.raw_recharge
WHERE TRIM(BOTH '''' FROM "用户ID") <> '';

CREATE OR REPLACE TABLE stg.stg_bet AS
SELECT
  DATE(COALESCE(TRY_STRPTIME("日期", '%Y/%m/%d'), TRY_STRPTIME("日期", '%Y-%m-%d'))) AS biz_date,
  TRIM(BOTH '''' FROM "ID") AS bet_id,
  "渠道来源" AS channel_source,
  TRIM(BOTH '''' FROM "用户ID") AS user_id,
  "用户手机号" AS user_phone,
  "游戏类型" AS game_type,
  "平台名称" AS platform_name,
  "子游戏名称" AS sub_game_name,
  "币种" AS currency,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("投注金额", '0')), ',', '', 'g') AS DOUBLE) AS bet_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("派彩金额", '0')), ',', '', 'g') AS DOUBLE) AS payout_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("会员盈亏", '0')), ',', '', 'g') AS DOUBLE) AS profit_amt_raw,
  dt,
  source_file,
  file_hash,
  ingested_at
FROM raw.raw_bet
WHERE TRIM(BOTH '''' FROM "用户ID") <> '';

CREATE OR REPLACE TABLE stg.stg_withdraw AS
SELECT
  TRIM(BOTH '''' FROM "提现ID") AS withdraw_id,
  TRIM(BOTH '''' FROM "用户ID") AS user_id,
  "渠道来源" AS channel_source,
  TRIM(BOTH '''' FROM "提现订单号") AS withdraw_order_id,
  TRIM(BOTH '''' FROM "平台订单号") AS platform_order_id,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("提现金额", '0')), ',', '', 'g') AS DOUBLE) AS withdraw_amt_raw,
  "首次提现" AS first_withdraw_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("手续费", '0')), ',', '', 'g') AS DOUBLE) AS fee_amt_raw,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("到账金额", '0')), ',', '', 'g') AS DOUBLE) AS arrival_amt_raw,
  "订单状态" AS status_raw,
  REPLACE(TRIM(BOTH '"' FROM COALESCE("拒绝原因", '')), '""', '"') AS reject_reason,
  REPLACE(TRIM(BOTH '"' FROM COALESCE("提现信息", '')), '""', '"') AS withdraw_info,
  "付款通道" AS pay_channel,
  COALESCE(TRY_STRPTIME("申请时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("申请时间", '%Y/%m/%d %H:%M:%S')) AS apply_time,
  COALESCE(TRY_STRPTIME("提交时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("提交时间", '%Y/%m/%d %H:%M:%S')) AS submit_time,
  COALESCE(TRY_STRPTIME("完成时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("完成时间", '%Y/%m/%d %H:%M:%S')) AS finish_time,
  DATE(COALESCE(TRY_STRPTIME("完成时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("完成时间", '%Y/%m/%d %H:%M:%S'))) AS biz_date,
  dt,
  source_file,
  file_hash,
  ingested_at
FROM raw.raw_withdraw
WHERE TRIM(BOTH '''' FROM "用户ID") <> '';

CREATE OR REPLACE TABLE stg.stg_bonus AS
SELECT
  TRIM(BOTH '''' FROM "用户UID") AS user_id,
  TRY_CAST(REGEXP_REPLACE(TRIM(BOTH '''' FROM COALESCE("彩金金额", '0')), ',', '', 'g') AS DOUBLE) AS bonus_amt_raw,
  COALESCE(TRY_STRPTIME("领取时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("领取时间", '%Y/%m/%d %H:%M:%S')) AS claim_time,
  "彩金类型" AS bonus_type,
  COALESCE(TRY_STRPTIME("注册时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("注册时间", '%Y/%m/%d %H:%M:%S')) AS register_time,
  "注册IP" AS register_ip,
  COALESCE(TRY_STRPTIME("首充时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("首充时间", '%Y/%m/%d %H:%M:%S')) AS first_recharge_time,
  "渠道号ID" AS channel_id,
  DATE(COALESCE(TRY_STRPTIME("领取时间", '%Y-%m-%d %H:%M:%S'), TRY_STRPTIME("领取时间", '%Y/%m/%d %H:%M:%S'))) AS biz_date,
  dt,
  source_file,
  file_hash,
  ingested_at
FROM raw.raw_bonus
WHERE TRIM(BOTH '''' FROM "用户UID") <> '';
