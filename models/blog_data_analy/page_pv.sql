--セッション数軸でのデータマートを作成
WITH page_data AS (
SELECT
  event_date,
  SUBSTRING(event_date, 1, 6) AS month,
  CASE WHEN params.key = 'page_title' THEN params.value.string_value ELSE null end AS page_title,
  user_pseudo_id,
  ga_session_id,
  event_name,
  time_stamp,
  LEAD(time_stamp,1) OVER (PARTITION BY ga_session_id ORDER BY time_stamp) AS lead_time_stamp
FROM
  (
    SELECT
      *,
      params,
      CASE WHEN params.key = 'ga_session_id' THEN params.value.int_value ELSE null end AS ga_session_id,
      CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_TRUNC(timestamp_micros(event_timestamp), SECOND), "Asia/Tokyo") AS datetime) AS time_stamp
    FROM
      `blogdata-371814.analytics_346792782.events_*`
      , UNNEST(event_params) AS params
  )
WHERE
  CAST(event_date AS INT64) BETWEEN 20231026 AND 20231026
)
SELECT
  event_date,
  month,
  page_title,
  COUNT(DISTINCT user_pseudo_id) AS uu_user_cnt,
  COUNT(DISTINCT ga_session_id) AS session_cnt,
  SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS page_view_cnt,
  timestamp_diff(MAX(lead_time_stamp), MIN(time_stamp), second) as diff_second,
FROM
  page_data
GROUP BY 
  1,2,3