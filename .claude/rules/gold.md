---
paths:
  - "health_unified_platform/**/gold/**"
  - "transformation_logic/databricks/gold/**"
---

# Gold Layer Rules

- Gold is views or aggregations — no raw data
- Always include: `date`, `user_id` (even if hardcoded for now), `source_system`
- Join silver tables — never read directly from bronze
- Document the business logic in a comment at the top of the SQL
- Scale-up note: add `depends_on: [silver_pipeline]` in DAB job config for correct ordering
