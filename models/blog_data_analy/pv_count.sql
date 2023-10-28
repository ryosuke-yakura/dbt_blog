--セッション数軸でのデータマートを作成
WITH total_data AS (
SELECT
  event_date,
  SUBSTRING(event_date, 1, 6) AS month,
  user_pseudo_id,
  CASE WHEN params.key = 'ga_session_id' THEN params.value.int_value ELSE null end AS ga_session_id,
  device.advertising_id,
  device.category,
  MIN(time_stamp) AS min_time_stamp,
  MAX(time_stamp) AS max_time_stamp,
  SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 end) AS page_view_cnt
FROM
  (
    SELECT
      *,
      CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_TRUNC(timestamp_micros(event_timestamp), SECOND), "Asia/Tokyo") AS datetime) AS time_stamp
    FROM
      `blogdata-371814.analytics_346792782.events_*`
  ),
  UNNEST(event_params) AS params
WHERE
  CAST(event_date AS INT64) BETWEEN 20231026 AND 20231026
GROUP BY 
  1,2,3,4,5,6
)
SELECT
  event_date,
  month,
  user_pseudo_id,
  ga_session_id,
  advertising_id,
  category,
  timestamp_diff(max_time_stamp, min_time_stamp, minute) as diff_second,
  page_view_cnt
FROM
  total_data