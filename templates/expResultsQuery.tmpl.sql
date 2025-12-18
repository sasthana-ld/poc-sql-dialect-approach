INSERT INTO {{ .FqTableArmAggregates }}(iteration_id, metric_version_id, arm_id, dimension_name, dimension_value, count_of_units, count_of_units_with_measurements, sum_of_unit_aggregates, sum_of_squares, metric_event_last_seen, analyzed_at) (
  WITH audience_for_each_metric AS (
   SELECT
      DISTINCT
          audience.experiment_iteration_id as iteration_id
          , audience.randomization_unit
          , audience.context_key
          , audience.context_attributes
          , audience.flag_key
          , audience.first_seen_timestamp
          , audience.iteration_ended_timestamp
          , audience.arm_id
      FROM {{ .FqTableAudience }} AS audience
          WHERE audience.experiment_iteration_id = '{{ .ExperimentIterationId }}'
  ), unit_aggregates as (
    SELECT 
        results.randomization_unit
        , results.context_key
        , results.context_attributes
        , results.flag_key
        , results.arm_id
        , {{ .AggregationFunction }}(results.event_value) AS metric_event_aggregate_value
        , COUNT(results.event_value) AS metric_event_count
        , MAX(results.event_timestamp) AS metric_event_last_seen
      FROM 
      (
          SELECT
          audience_for_each_metric.randomization_unit
          , audience_for_each_metric.context_key
          , audience_for_each_metric.context_attributes
          , audience_for_each_metric.flag_key
          , audience_for_each_metric.arm_id
          , metric_events.event_timestamp
          , metric_events.event_value AS event_value
          FROM audience_for_each_metric
              LEFT JOIN ({{ .DataSource }}) AS metric_events
              ON audience_for_each_metric.context_key = metric_events.context_key
              AND audience_for_each_metric.first_seen_timestamp <=  metric_events.event_timestamp
      ) AS results
      GROUP BY 1, 2, 3, 4, 5
  ), unit_dimensions as (
      {{ .Dialect.UnitDimensionsCTE .UnitDimensionsParams }}
  ) SELECT
          '{{ .ExperimentIterationId }}' as iteration_id,
          '{{ .MetricVersionId }}' as metric_version_id,
          arm_id,
          dimension_name,
          dimension_value,
          COUNT(*) AS count_of_units,
          COUNT(metric_event_aggregate_value) AS count_of_units_with_measurements,
          SUM(metric_event_aggregate_value) AS sum_of_unit_aggregates,
          SUM({{ .Dialect.Square "metric_event_aggregate_value" }}) AS sum_of_squares,
          MAX(metric_event_last_seen) as metric_event_last_seen,
          MIN({{ .Dialect.CurrentTimestamp }}) as analyzed_at
      FROM unit_dimensions
      GROUP BY 1, 2, 3, 4, 5
      UNION ALL
      SELECT
         '{{ .ExperimentIterationId }}' as iteration_id,
         '{{ .MetricVersionId }}' as metric_version_id,
         arm_id,
         NULL AS dimension_name,
         NULL AS dimension_value,
         COUNT(*) AS count_of_units,
         COUNT(metric_event_aggregate_value) AS count_of_units_with_measurements,
         SUM(metric_event_aggregate_value) AS sum_of_unit_aggregates,
         SUM({{ .Dialect.Square "metric_event_aggregate_value" }}) AS sum_of_squares,
         MAX(metric_event_last_seen) as metric_event_last_seen,
         MIN({{ .Dialect.CurrentTimestamp }}) as analyzed_at
      FROM unit_aggregates
      GROUP BY 1, 2, 3, 4, 5
)
