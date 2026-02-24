# Genie Space API Examples

Python examples for managing Databricks Genie Spaces programmatically via REST API.

## Setup

```bash
pip install -r requirements.txt

export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
```

Or use the Databricks CLI for auth:
```python
from config import init_from_cli
init_from_cli(profile="e2-field")
```

## API Reference

| Operation | Method | Endpoint | Docs |
|---|---|---|---|
| Create Space | POST | `/api/2.0/genie/spaces` | [createspace](https://docs.databricks.com/api/workspace/genie/createspace) |
| List Spaces | GET | `/api/2.0/genie/spaces` | [listspaces](https://docs.databricks.com/api/workspace/genie/listspaces) |
| Get/Export Space | GET | `/api/2.0/genie/spaces/{space_id}?include_serialized_space=true` | [getspace](https://docs.databricks.com/api/workspace/genie/getspace) |
| Update Space | PATCH | `/api/2.0/genie/spaces/{space_id}` | [conversation-api](https://docs.databricks.com/aws/en/genie/conversation-api) |
| Delete Space | DELETE | `/api/2.0/genie/spaces/{space_id}` | [genie](https://docs.databricks.com/api/workspace/genie) |
| Get Permissions | GET | `/api/2.0/permissions/genie/{space_id}` | [permissions/get](https://docs.databricks.com/api/workspace/permissions/get) |
| List Permission Levels | GET | `/api/2.0/permissions/genie/{space_id}/permissionLevels` | [getpermissionlevels](https://docs.databricks.com/api/workspace/permissions/getpermissionlevels) |
| Update Permissions | PATCH | `/api/2.0/permissions/genie/{space_id}` | [permissions/update](https://docs.databricks.com/api/workspace/permissions/update) |
| Replace Permissions | PUT | `/api/2.0/permissions/genie/{space_id}` | [permissions/set](https://docs.databricks.com/api/workspace/permissions/set) |
| Execute SQL | POST | `/api/2.0/sql/statements/` | [executestatement](https://docs.databricks.com/api/workspace/statementexecution/executestatement) |

## Examples

### 1. Add/Update Metrics (`01_metrics.py`)

Two approaches for managing metrics:

- **Inline Measures** — Define metrics directly in `serialized_space.instructions.sql_snippets.measures` via `POST` or `PATCH`. Each measure has an `id`, `sql` (array), and `display_name`. Best for space-specific calculations.
- **Unity Catalog Metric Views (Recommended)** — Create governed, reusable metrics via DDL and attach them to `data_sources.metric_views`. The DDL uses `version: 1.1` YAML with `source:` inside the block (not `$$ FROM`). Shared across Genie, Dashboards, and SQL Alerts.

### 2. Add/Update Data Sources (`02_data_sources.py`)

Data sources are managed through the Genie CRUD API:

- `GET` with `include_serialized_space=true` to export current config
- `PATCH` with modified `serialized_space.data_sources` to update
- Supports managed tables, external tables, views, materialized views, and metric views
- Up to 30 tables/views per space; all identifiers use three-level namespace (`catalog.schema.table`)

### 3. Add/Edit Roles and Permissions (`03_permissions.py`)

Permissions use the Workspace Permissions API with object type `genie` (not `genie-spaces`):

- `GET /api/2.0/permissions/genie/{space_id}` — returns `all_permissions` with inherited info
- `PATCH` — additive; use `permission_level` directly on each ACL entry
- `PUT` — destructive replace of all permissions
- **Levels:** CAN_READ, CAN_RUN, CAN_EDIT, CAN_MANAGE
- Assignable to users, groups, or service principals

### 4. List/Add/Update Context and Docs (`04_context.py`)

Context is managed through the `serialized_space` JSON payload:

- `GET` with `include_serialized_space=true` to export (requires CAN_EDIT)
- `PATCH` to update

Supported context sources: `text_instructions` (1 item, content is a string array), `example_question_sqls` (static or parameterized for "Trusted" badge), `sql_snippets` (filters, expressions, measures), `config.sample_questions`, and `column_configs` on tables (entity matching, format assistance).

### 5. Clean Up (`05_cleanup.py`)

Finds and deletes Genie Spaces created by the examples. Optionally drops test tables (`--tables`) and the schema (`--schema`).

### Demo (`demo.py`)

Step-by-step demo that creates two persistent Genie Spaces — one with inline measures and one with metric views — walking through all four examples. Nothing is cleaned up; run `05_cleanup.py` when done.

## Metric View DDL Syntax

```sql
CREATE OR REPLACE VIEW catalog.schema.mv_name
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Description"

  source: catalog.schema.source_table

  dimensions:
    - name: Dimension Name
      expr: column_expression

  measures:
    - name: Measure Name
      expr: AGG(column)
$$;
```

Key: `source:` goes inside the YAML block (not `$$ FROM table`), and the block ends with `$$;`.

## Serialized Space Schema (v2)

Key constraints discovered during testing:

- **`version: 2`** is required in every `serialized_space`
- **Tables sorted alphabetically** by `identifier` field
- **All ID-bearing lists sorted** by `id` (measures, filters, expressions, example_question_sqls, sample_questions, text_instructions)
- **`text_instructions`** allows only 1 item; append to the `content` array for additional text
- **Table field** is `identifier` (not `table_identifier`)
- **Updates use PATCH** (not PUT) on `/api/2.0/genie/spaces/{space_id}`
- **Permission PATCH format** uses `permission_level` directly on ACL entries (not nested in `all_permissions`)

## Testing

```bash
python3 run_tests.py
```

Runs integration tests against a live workspace, creating/updating/deleting test spaces for each example.

## References

- [Genie API Reference](https://docs.databricks.com/api/workspace/genie) — REST API for creating/managing Genie Spaces
- [Genie Conversation API](https://docs.databricks.com/aws/en/genie/conversation-api) — serialized_space schema, context management
- [Set Up a Genie Space](https://docs.databricks.com/aws/en/genie/set-up) — UI-based setup guide, permissions, context types
- [Permissions API](https://docs.databricks.com/api/workspace/permissions) — workspace-level permissions (object type: `genie`)
- [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution) — execute SQL for DDL, queries
- [SQL Execution Tutorial](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial) — step-by-step guide for the Statement Execution API
- [Metric Views](https://docs.databricks.com/aws/en/metric-views/) — governed, reusable metrics in Unity Catalog
- [Create a Metric View](https://docs.databricks.com/aws/en/metric-views/create) — DDL syntax for metric views
- [Model Metric View Data](https://docs.databricks.com/aws/en/metric-views/data-modeling/) — data modeling patterns for metrics
