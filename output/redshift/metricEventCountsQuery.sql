INSERT INTO mydb.myschema.event_counts(data_source_id, event_key, rollup_hour_ts, event_count, analyzed_at) (
  SELECT
        'datasource-123' as data_source_id
        , event_key
        , DATE_TRUNC('hour', event_timestamp) as rollup_hour_ts
        , COUNT(*) as event_count
        , GETDATE() as analyzed_at
  FROM (SELECT * FROM mydb.myschema.events WHERE event_type = 'metric')
  WHERE event_timestamp >= CURRENT_DATE - interval '1 day'
    AND event_key IS NOT NULL
    AND event_key <> ''
    AND length(event_key) < 1025
    AND event_key NOT LIKE '%\u0000%'
  GROUP BY event_key, rollup_hour_ts
  LIMIT 1000000
);

