INSERT INTO mydb.myschema.events_last_seen(data_source_id, event_key, context_kinds, last_received_ts, analyzed_at) (
    SELECT
      'datasource-123' as data_source_id
      , event_key
      , ARRAY_AGG(DISTINCT context_kind) as context_kinds
      , MAX(event_timestamp) as last_received_ts
      , CURRENT_TIMESTAMP() as analyzed_at
    FROM (SELECT * FROM mydb.myschema.events WHERE event_type = 'metric')
    WHERE event_timestamp >= '2024-01-01T00:00:00Z'
      AND event_timestamp > CURRENT_DATE - interval '7 days'
      AND event_key IS NOT NULL
      AND event_key <> ''
      AND length(event_key) < 1025
      AND event_key NOT LIKE '%\u0000%'
    GROUP BY event_key
    LIMIT 100000
);

