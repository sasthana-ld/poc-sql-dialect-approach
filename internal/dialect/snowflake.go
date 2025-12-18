package dialect

import "fmt"

type SnowflakeDialect struct{}

func NewSnowflakeDialect() *SnowflakeDialect {
	return &SnowflakeDialect{}
}

func (d *SnowflakeDialect) Name() string {
	return "Snowflake"
}

func (d *SnowflakeDialect) Square(col string) string {
	return fmt.Sprintf("SQUARE(%s)", col)
}

func (d *SnowflakeDialect) CurrentTimestamp() string {
	return "CURRENT_TIMESTAMP()"
}

func (d *SnowflakeDialect) IfElse(condition, trueVal, falseVal string) string {
	return fmt.Sprintf("IFF(%s, %s, %s)", condition, trueVal, falseVal)
}

func (d *SnowflakeDialect) DateTrunc(unit, col string) string {
	return fmt.Sprintf("DATE_TRUNC('%s', %s)", unit, col)
}

func (d *SnowflakeDialect) UnitDimensionsCTE(params UnitDimensionsCTEParams) string {
	return fmt.Sprintf(`SELECT
          '%s' as iteration_id,
          '%s' as metric_version_id,
          ua.arm_id,
          id.dimension_name,
          GET_PATH(PARSE_JSON(ua.context_attributes), id.dimension_name) AS dimension_value,
          ua.metric_event_aggregate_value,
          ua.metric_event_last_seen
      FROM unit_aggregates ua
      JOIN %s id
      WHERE GET_PATH(PARSE_JSON(ua.context_attributes), id.dimension_name) IS NOT NULL AND id.iteration_id = '%s'`,
		params.ExperimentIterationId,
		params.MetricVersionId,
		params.FqTableIterationDimensions,
		params.ExperimentIterationId,
	)
}
