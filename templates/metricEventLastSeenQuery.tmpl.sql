INSERT INTO {{ .FqTableEventsLastSeen }}(data_source_id, event_key, context_kinds, last_received_ts, analyzed_at) (
    SELECT
      '{{ .DataSourceId }}' as data_source_id
      , event_key
      , {{ .ContextKinds }} as context_kinds
      , MAX(event_timestamp) as last_received_ts
      , {{ .Dialect.CurrentTimestamp }} as analyzed_at
    FROM ({{.DataSource }})
    WHERE event_timestamp >= '{{ .EventLastSeenStartTime }}'
      AND event_timestamp > CURRENT_DATE - interval '7 days'
      AND event_key IS NOT NULL
      AND event_key <> ''
      AND length(event_key) < 1025
      AND event_key NOT LIKE '%\u0000%'
    GROUP BY event_key
    LIMIT 100000
);

