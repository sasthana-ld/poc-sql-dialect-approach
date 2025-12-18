package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"poc-dialect-helpers/internal/dialect"
)

type QueryVars struct {
	FqTableArmAggregates       string
	FqTableAudience            string
	FqTableMetricDefinitions   string
	FqViewMetricEvents         string
	FqTableIterationDimensions string
	FqTableEventCounts         string
	FqTableEventsLastSeen      string
	ExperimentIterationId      string
	MetricVersionId            string
	AggregationFunction        string
	DataSource                 string
	DataSourceId               string
	ContextKinds               string
	EventLastSeenStartTime     string
	Dialect                    dialect.DialectHelpers
	UnitDimensionsParams       dialect.UnitDimensionsCTEParams
}

func main() {
	fmt.Println("=============================================================================")
	fmt.Println("Dialect Helpers PoC - Generating SQL for Snowflake and Redshift")
	fmt.Println("=============================================================================")
	fmt.Println()

	templates := []string{
		"expResultsQuery.tmpl.sql",
		"unitAggregatesQuery.tmpl.sql",
		"metricEventCountsQuery.tmpl.sql",
		"metricEventLastSeenQuery.tmpl.sql",
	}

	snowflakeDialect := dialect.NewSnowflakeDialect()
	redshiftDialect := dialect.NewRedshiftDialect()

	baseVars := QueryVars{
		FqTableArmAggregates:       "mydb.myschema.arm_aggregates",
		FqTableAudience:            "mydb.myschema.audience",
		FqTableMetricDefinitions:   "mydb.myschema.metric_definitions",
		FqViewMetricEvents:         "mydb.myschema.metric_events",
		FqTableIterationDimensions: "mydb.myschema.iteration_dimensions",
		FqTableEventCounts:         "mydb.myschema.event_counts",
		FqTableEventsLastSeen:      "mydb.myschema.events_last_seen",
		ExperimentIterationId:      "exp-iteration-12345",
		MetricVersionId:            "metric-version-67890",
		AggregationFunction:        "SUM",
		DataSource:                 "SELECT * FROM mydb.myschema.events WHERE event_type = 'metric'",
		DataSourceId:               "datasource-123",
		ContextKinds:               "ARRAY_AGG(DISTINCT context_kind)",
		EventLastSeenStartTime:     "2024-01-01T00:00:00Z",
	}

	for _, tmplFile := range templates {
		fmt.Printf("Processing: %s\n", tmplFile)

		snowflakeVars := baseVars
		snowflakeVars.Dialect = snowflakeDialect
		snowflakeVars.UnitDimensionsParams = dialect.UnitDimensionsCTEParams{
			ExperimentIterationId:      baseVars.ExperimentIterationId,
			MetricVersionId:            baseVars.MetricVersionId,
			FqTableIterationDimensions: baseVars.FqTableIterationDimensions,
		}

		redshiftVars := baseVars
		redshiftVars.Dialect = redshiftDialect
		redshiftVars.UnitDimensionsParams = dialect.UnitDimensionsCTEParams{
			ExperimentIterationId:      baseVars.ExperimentIterationId,
			MetricVersionId:            baseVars.MetricVersionId,
			FqTableIterationDimensions: baseVars.FqTableIterationDimensions,
		}

		if err := processTemplate(tmplFile, snowflakeVars, "snowflake"); err != nil {
			fmt.Printf("  ERROR (Snowflake): %v\n", err)
			continue
		}

		if err := processTemplate(tmplFile, redshiftVars, "redshift"); err != nil {
			fmt.Printf("  ERROR (Redshift): %v\n", err)
			continue
		}
	}

	fmt.Println()
	fmt.Println("=============================================================================")
	fmt.Println("✓ Done! Generated SQL files in output/snowflake/ and output/redshift/")
	fmt.Println("=============================================================================")
}

func processTemplate(tmplFile string, vars QueryVars, dialectName string) error {
	tmplPath := filepath.Join("templates", tmplFile)
	content, err := os.ReadFile(tmplPath)
	if err != nil {
		return fmt.Errorf("failed to read template: %w", err)
	}

	tmpl, err := template.New(tmplFile).Parse(string(content))
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	outputDir := filepath.Join("output", dialectName)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	outputFileName := strings.TrimSuffix(tmplFile, ".tmpl.sql") + ".sql"
	outputFile := filepath.Join(outputDir, outputFileName)
	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	if err := tmpl.Execute(f, vars); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	return nil
}
