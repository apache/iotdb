SELECT
  COUNT(DISTINCT MDaysUsers.user_pseudo_id) AS n_day_inactive_users_count
FROM
  (
    SELECT
      user_pseudo_id
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS T
    CROSS JOIN
      UNNEST(T.event_params) AS event_params
    WHERE
      event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
      AND event_timestamp > UNIX_MICROS(TIMESTAMP_SUB(TIMESTAMP('2021-01-07 23:59:59'), INTERVAL 7 DAY))
      AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210107'
  ) AS MDaysUsers
LEFT JOIN
  (
    SELECT
      user_pseudo_id
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS T
    CROSS JOIN
      UNNEST(T.event_params) AS event_params
    WHERE
      event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
      AND event_timestamp > UNIX_MICROS(TIMESTAMP_SUB(TIMESTAMP('2021-01-07 23:59:59'), INTERVAL 2 DAY))
      AND _TABLE_SUFFIX BETWEEN '20210105' AND '20210107'
  ) AS NDaysUsers
ON MDaysUsers.user_pseudo_id = NDaysUsers.user_pseudo_id
WHERE
  NDaysUsers.user_pseudo_id IS NULL;