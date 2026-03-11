CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops."用户状态总览_每日" AS
SELECT
  v.*,
  NOW() AS "入库时间"
FROM ops."用户状态总览" v
WHERE 1 = 0;

DELETE FROM ops."用户状态总览_每日"
WHERE "统计日期" = ?::DATE;

INSERT INTO ops."用户状态总览_每日"
SELECT
  v.*,
  NOW() AS "入库时间"
FROM ops."用户状态总览" v
WHERE v."统计日期" = ?::DATE
  AND v."注册时间" IS NOT NULL
  AND DATE(v."注册时间") <= v."统计日期";
