INSERT INTO {{ .FqTableEventCounts }}(data_source_id, event_key, rollup_hour_ts, event_count, analyzed_at) (
  SELECT
        '{{ .DataSourceId }}' as data_source_id
        , event_key
        , {{ .Dialect.DateTrunc "hour" "event_timestamp" }} as rollup_hour_ts
        , COUNT(*) as event_count
        , {{ .Dialect.CurrentTimestamp }} as analyzed_at
  FROM ({{ .DataSource }})
  WHERE event_timestamp >= CURRENT_DATE - interval '1 day'
    AND event_key IS NOT NULL
    AND event_key <> ''
    AND length(event_key) < 1025
    AND event_key NOT LIKE '%\u0000%'
  GROUP BY event_key, rollup_hour_ts
  LIMIT 1000000
);

