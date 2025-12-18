package dialect

type DialectHelpers interface {
	Name() string
	Square(col string) string
	CurrentTimestamp() string
	IfElse(condition, trueVal, falseVal string) string
	DateTrunc(unit, col string) string
	UnitDimensionsCTE(params UnitDimensionsCTEParams) string
}

type UnitDimensionsCTEParams struct {
	ExperimentIterationId      string
	MetricVersionId            string
	FqTableIterationDimensions string
}
