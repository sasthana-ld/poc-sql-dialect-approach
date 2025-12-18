package dialect

import "fmt"

type RedshiftDialect struct{}

func NewRedshiftDialect() *RedshiftDialect {
	return &RedshiftDialect{}
}

func (d *RedshiftDialect) Name() string {
	return "Redshift"
}

func (d *RedshiftDialect) Square(col string) string {
	return fmt.Sprintf("POWER(%s, 2)", col)
}

func (d *RedshiftDialect) CurrentTimestamp() string {
	return "GETDATE()"
}

func (d *RedshiftDialect) IfElse(condition, trueVal, falseVal string) string {
	return fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", condition, trueVal, falseVal)
}

func (d *RedshiftDialect) DateTrunc(unit, col string) string {
	return fmt.Sprintf("DATE_TRUNC('%s', %s)", unit, col)
}

func (d *RedshiftDialect) UnitDimensionsCTE(params UnitDimensionsCTEParams) string {
	return fmt.Sprintf(`SELECT
          '%s' as iteration_id,
          '%s' as metric_version_id,
          ua.arm_id,
          id.dimension_name,
          val::varchar AS dimension_value,
          ua.metric_event_aggregate_value,
          ua.metric_event_last_seen
      FROM unit_aggregates ua
      JOIN %s id ON id.iteration_id = '%s'
      , UNPIVOT JSON_PARSE(ua.context_attributes) AS val AT attr
      WHERE attr = id.dimension_name`,
		params.ExperimentIterationId,
		params.MetricVersionId,
		params.FqTableIterationDimensions,
		params.ExperimentIterationId,
	)
}
