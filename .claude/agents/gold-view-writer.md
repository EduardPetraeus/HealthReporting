---
name: gold-view-writer
description: Build a gold layer view or aggregation from silver tables. Use when asked to create a gold entity, dashboard view, or analytical summary.
---

# Gold View Writer

Build a production-ready gold layer view.

## Steps

1. Clarify (or infer from context): what business question does this view answer?
2. Read relevant silver tables to understand available columns
3. Read existing gold SQL in `transformation_logic/databricks/gold/sql/` for pattern reference
4. Generate the gold view:

```sql
-- gold/<entity_name>.sql
-- Business purpose: <what question this answers>
-- Sources: silver.<table1>, silver.<table2>

CREATE OR REPLACE VIEW gold.<entity_name> AS
SELECT
  CAST(date AS DATE)                    AS date,
  'claus_petraeus'                      AS user_id,  -- TODO: replace with dim_user join
  source_system,
  <business_columns>,
  current_timestamp()                   AS _created_at
FROM silver.<primary_table>
LEFT JOIN silver.<secondary_table> USING (date, source_system)
WHERE date IS NOT NULL;
```

## Rules
- Gold is always views — never materialized tables (unless performance requires it, document why)
- Always include: `date`, `user_id`, `source_system`
- Join silver only — never read from bronze
- Add a `-- Scale-up note` comment for any simplifications made

## Output
- SQL file in `transformation_logic/databricks/gold/sql/`
- YAML entry in `health_environment/config/databricks/gold/`
