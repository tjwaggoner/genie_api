"""
Example 1: Add/Update Metrics in a Genie Space

Demonstrates two approaches:
  A) Inline measures defined directly in the serialized_space sql_snippets
  B) Unity Catalog Metric Views created via SQL DDL and attached to the space

Docs:
  Genie API:        https://docs.databricks.com/api/workspace/genie
  Create Space:     https://docs.databricks.com/api/workspace/genie/createspace
  Metric Views:     https://docs.databricks.com/aws/en/metric-views/
  Create Metric:    https://docs.databricks.com/aws/en/metric-views/create
  Genie Context:    https://docs.databricks.com/aws/en/genie/conversation-api
"""

import json
from config import api_request, execute_sql, gen_id, CATALOG, SCHEMA


# ---------------------------------------------------------------------------
# Approach A: Inline Measures via Genie CRUD API
# ---------------------------------------------------------------------------

def create_space_with_inline_measures(warehouse_id):
    """
    Create a Genie Space with inline metric measures.

    POST /api/2.0/genie/spaces
    Docs: https://docs.databricks.com/api/workspace/genie/createspace
    """
    payload = {
        "title": "Finance Metrics Space",
        "description": "Genie space with inline financial measures",
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps({
            "version": 2,
            "data_sources": {
                "tables": [
                    {"identifier": f"{CATALOG}.{SCHEMA}.invoices"}
                ]
            },
            "instructions": {
                "text_instructions": [
                    {
                        "id": gen_id(),
                        "content": ["This space answers questions about financial invoices."]
                    }
                ],
                "sql_snippets": {
                    "measures": sorted([
                        {
                            "id": gen_id(),
                            "sql": ["SUM(amount)"],
                            "display_name": "total_revenue"
                        },
                        {
                            "id": gen_id(),
                            "sql": ["COUNT(DISTINCT invoice_id)"],
                            "display_name": "invoice_count"
                        },
                        {
                            "id": gen_id(),
                            "sql": ["AVG(amount)"],
                            "display_name": "avg_invoice_amount"
                        }
                    ], key=lambda x: x["id"])
                }
            }
        })
    }
    return api_request("POST", "/api/2.0/genie/spaces", json_data=payload)


def update_space_measures(space_id):
    """
    Update inline measures on an existing Genie Space.

    GET  /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    PATCH /api/2.0/genie/spaces/{space_id}
    Docs: https://docs.databricks.com/api/workspace/genie/getspace
    """
    # Step 1: GET current config
    current = api_request(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        params={"include_serialized_space": "true"},
    )
    config = json.loads(current["serialized_space"])

    # Step 2: Modify measures
    config.setdefault("instructions", {}).setdefault("sql_snippets", {})
    config["instructions"]["sql_snippets"]["measures"] = sorted([
        {
            "id": gen_id(),
            "sql": ["SUM(amount)"],
            "display_name": "total_revenue"
        },
        {
            "id": gen_id(),
            "sql": ["SUM(amount) / NULLIF(COUNT(DISTINCT invoice_id), 0)"],
            "display_name": "revenue_per_invoice"
        },
    ], key=lambda x: x["id"])

    # Step 3: PATCH updated config (all ID-bearing lists must be sorted)
    return api_request("PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data={
        "serialized_space": json.dumps(config),
    })


# ---------------------------------------------------------------------------
# Approach B: Unity Catalog Metric Views (Recommended for Enterprise)
# ---------------------------------------------------------------------------

def create_metric_view():
    """
    Create a metric view in Unity Catalog via SQL DDL.
    Requires the Metric Views feature to be enabled on the workspace.

    POST /api/2.0/sql/statements/
    Docs: https://docs.databricks.com/aws/en/metric-views/create
    """
    ddl = (
        f"CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_invoice\n"
        f"WITH METRICS\n"
        f"LANGUAGE YAML\n"
        f"AS $$\n"
        f"  version: 1.1\n"
        f"  comment: \"Invoice financial metrics\"\n"
        f"\n"
        f"  source: {CATALOG}.{SCHEMA}.invoices\n"
        f"\n"
        f"  dimensions:\n"
        f"    - name: Company ID\n"
        f"      expr: company_id\n"
        f"\n"
        f"    - name: Fiscal Quarter\n"
        f"      expr: fiscal_quarter\n"
        f"\n"
        f"    - name: Status\n"
        f"      expr: status\n"
        f"\n"
        f"  measures:\n"
        f"    - name: Total Revenue\n"
        f"      expr: SUM(amount)\n"
        f"\n"
        f"    - name: Invoice Count\n"
        f"      expr: COUNT(DISTINCT invoice_id)\n"
        f"$$;"
    )
    return execute_sql(ddl)


def attach_metric_view_to_space(space_id):
    """
    Add a metric view to an existing Genie Space's data sources.

    GET   /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    PATCH /api/2.0/genie/spaces/{space_id}
    Docs: https://docs.databricks.com/api/workspace/genie/getspace
    """
    current = api_request(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        params={"include_serialized_space": "true"},
    )
    config = json.loads(current["serialized_space"])

    config.setdefault("data_sources", {})
    config["data_sources"].setdefault("metric_views", [])
    config["data_sources"]["metric_views"].append(
        {"identifier": f"{CATALOG}.{SCHEMA}.mv_invoice"}
    )

    return api_request("PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data={
        "serialized_space": json.dumps(config),
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from config import init_from_cli, WAREHOUSE_ID
    init_from_cli()

    wh = WAREHOUSE_ID or "cd3b290bff658fa3"

    print("=== Approach A: Inline Measures ===")
    print("Creating space with inline measures...")
    result = create_space_with_inline_measures(wh)
    new_space_id = result["space_id"]
    print(f"Space created: {new_space_id}")

    print("\nUpdating inline measures...")
    update_space_measures(new_space_id)
    print("Measures updated.")

    print("\n=== Approach B: Metric Views ===")
    print("Creating metric view via DDL...")
    mv = create_metric_view()
    state = mv.get("status", {}).get("state", "")
    if state == "SUCCEEDED":
        print(f"Metric view created: {CATALOG}.{SCHEMA}.mv_invoice")
        print("\nAttaching metric view to space...")
        attach_metric_view_to_space(new_space_id)
        print("Metric view attached.")
    else:
        print(f"Metric view DDL not supported on this workspace (state: {state})")
