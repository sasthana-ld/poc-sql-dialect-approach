WITH audience_for_each_metric AS (
    SELECT
        DISTINCT
        audience.experiment_iteration_id as iteration_id
               , metric_definitions.metric_version_id
               , audience.randomization_unit
               , audience.context_key
               , audience.context_attributes
               , audience.flag_key
               , audience.first_seen_timestamp
               , audience.iteration_ended_timestamp
               , audience.arm_id
               , metric_definitions.aggregation_type
               , metric_definitions.is_numeric
               , metric_definitions.metric_event_key
    FROM {{ .FqTableAudience }} AS audience
    INNER JOIN {{ .FqTableMetricDefinitions }} AS metric_definitions
ON audience.experiment_iteration_id = metric_definitions.iteration_id
    )

SELECT
    results.iteration_id
     , results.metric_version_id
     , results.randomization_unit
     , results.context_key
     , results.context_attributes
     , results.flag_key
     , results.arm_id
     , results.aggregation_type
     , CASE
           WHEN results.aggregation_type = 'average'
               THEN avg(results.event_value)
           WHEN results.aggregation_type = 'sum'
               THEN sum(results.event_value)
    END AS metric_event_value_aggregate
     , COUNT(results.event_value) AS metric_event_count
     , MAX(results.received_time) AS metric_event_last_seen
FROM
    (
        SELECT
            audience_for_each_metric.iteration_id
             , audience_for_each_metric.metric_version_id
             , audience_for_each_metric.randomization_unit
             , audience_for_each_metric.context_key
             , audience_for_each_metric.context_attributes
             , audience_for_each_metric.flag_key
             , audience_for_each_metric.arm_id
             , audience_for_each_metric.aggregation_type
             , metric_events.received_time
             , {{ .Dialect.IfElse "audience_for_each_metric.is_numeric" "metric_events.event_value" (.Dialect.IfElse "metric_events.received_time IS NOT NULL" "1.0" "NULL") }} AS event_value
        FROM audience_for_each_metric

                 LEFT JOIN {{ .FqViewMetricEvents }} AS metric_events
        ON audience_for_each_metric.metric_event_key = metric_events.event_key
            AND audience_for_each_metric.context_key = metric_events.context_key
            AND audience_for_each_metric.randomization_unit = metric_events.context_kind
            AND NOT (metric_events.event_value IS NULL AND audience_for_each_metric.is_numeric)
            AND (metric_events.received_time IS NULL
            OR
            (audience_for_each_metric.first_seen_timestamp <=  metric_events.received_time
            AND
            (
            audience_for_each_metric.iteration_ended_timestamp >  metric_events.received_time
            OR
            audience_for_each_metric.iteration_ended_timestamp IS NULL
            )
            )
            )
    ) AS results

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
