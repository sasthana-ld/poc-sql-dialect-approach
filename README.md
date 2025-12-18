# SQL Dialect Abstraction - Proof of Concept

This PoC demonstrates that one shared SQL template can generate correct warehouse-specific SQL for both Snowflake and Redshift.

## Problem

Snowflake and Redshift use different SQL syntax. Supporting multiple warehouses traditionally requires maintaining separate copies of SQL templates.

**Examples:**
- Snowflake: `SQUARE(x)` vs Redshift: `POWER(x, 2)`
- Snowflake: `IFF(condition, a, b)` vs Redshift: `CASE WHEN condition THEN a ELSE b END`
- Snowflake: `GET_PATH(PARSE_JSON(col), key)` vs Redshift: requires UNPIVOT pattern

## Solution

Use a DialectHelpers interface where each warehouse implements its own SQL syntax.

```
                    ┌─────────────────────────┐
                    │   Shared SQL Template   │
                    │  {{ .Dialect.Square }}  │
                    └───────────┬─────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
          ┌──────────────────┐    ┌──────────────────┐
          │ SnowflakeDialect │    │ RedshiftDialect  │
          │  Square() =>     │    │  Square() =>     │
          │  "SQUARE(x)"     │    │  "POWER(x, 2)"   │
          └──────────────────┘    └──────────────────┘
                    │                       │
                    ▼                       ▼
          ┌──────────────────┐    ┌──────────────────┐
          │  Snowflake SQL   │    │   Redshift SQL   │
          └──────────────────┘    └──────────────────┘
```

## Repository Structure

```
poc-dialect-helpers/
├── internal/dialect/
│   ├── dialect.go          Interface definition
│   ├── snowflake.go        Snowflake implementation
│   └── redshift.go         Redshift implementation
├── templates/
│   ├── expResultsQuery.tmpl.sql
│   ├── unitAggregatesQuery.tmpl.sql
│   ├── metricEventCountsQuery.tmpl.sql
│   └── metricEventLastSeenQuery.tmpl.sql
├── output/
│   ├── snowflake/          Generated Snowflake SQL
│   └── redshift/           Generated Redshift SQL
└── main.go                 PoC runner
```

## How to Run

```bash
# Generate SQL for both warehouses
go run main.go

# View generated Snowflake SQL
cat output/snowflake/expResultsQuery.sql

# View generated Redshift SQL
cat output/redshift/expResultsQuery.sql

# Compare the differences
diff output/snowflake/expResultsQuery.sql output/redshift/expResultsQuery.sql
```

## Example

**Template (ONE file):**
```sql
SELECT
    SUM({{ .Dialect.Square "metric_value" }}) AS sum_of_squares,
    MIN({{ .Dialect.CurrentTimestamp }}) AS analyzed_at
FROM results
```

**Snowflake Output:**
```sql
SELECT
    SUM(SQUARE(metric_value)) AS sum_of_squares,
    MIN(CURRENT_TIMESTAMP()) AS analyzed_at
FROM results
```

**Redshift Output:**
```sql
SELECT
    SUM(POWER(metric_value, 2)) AS sum_of_squares,
    MIN(GETDATE()) AS analyzed_at
FROM results
```

## Key Challenge: Dynamic JSON Path Extraction

Snowflake allows dynamic JSON key lookup using a column value:
```sql
GET_PATH(PARSE_JSON(json_col), dimension_name)  -- dimension_name is a column
```

Redshift does NOT support this. It only accepts literal strings for JSON paths.

**Solution:** Use UNPIVOT to restructure the query.

**Snowflake Approach:**
```sql
SELECT
    dimension_name,
    GET_PATH(PARSE_JSON(context_attributes), dimension_name) AS value
FROM unit_aggregates ua
JOIN iteration_dimensions id
WHERE GET_PATH(PARSE_JSON(context_attributes), dimension_name) IS NOT NULL
```

**Redshift Approach:**
```sql
SELECT
    dimension_name,
    val::varchar AS value
FROM unit_aggregates ua
JOIN iteration_dimensions id
, UNPIVOT JSON_PARSE(context_attributes) AS val AT attr
WHERE attr = dimension_name
```

Both produce the same result using different SQL structure.

## Function Translations

| Template Syntax | Snowflake Output | Redshift Output |
|----------------|------------------|-----------------|
| `{{ .Dialect.Square "x" }}` | `SQUARE(x)` | `POWER(x, 2)` |
| `{{ .Dialect.CurrentTimestamp }}` | `CURRENT_TIMESTAMP()` | `GETDATE()` |
| `{{ .Dialect.IfElse "a" "b" "c" }}` | `IFF(a, b, c)` | `CASE WHEN a THEN b ELSE c END` |

## What This PoC Proves

**One Template, Multiple Warehouses**
- A single template file can generate correct SQL for different warehouses
- No need to duplicate SQL templates

**Simple Function Translations Work**
- Functions like SQUARE, IFF, CURRENT_TIMESTAMP translate cleanly

**Complex Structural Changes Work**
- The UNPIVOT pattern proves we can handle query-shape differences, not just function name swaps

**Scalable Pattern**
- Adding a new warehouse requires ~50 lines of dialect implementation
- Does not require duplicating all SQL templates

## Production Implementation

This PoC validates the approach. In production (warehouse-task-runner):

1. SQL is generated in memory, not saved to files
2. SQL is executed immediately on the warehouse
3. No output folder exists
4. The main.go file is only for validation and is not used in production

## Next Steps

**Phase 1:** Implement dialect abstraction in warehouse-task-runner for Snowflake
**Phase 2:** Add Redshift support and validate on actual Redshift cluster

