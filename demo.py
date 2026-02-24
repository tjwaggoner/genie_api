"""
Demo runner: creates two persistent Genie Spaces and walks through each example.

Space A — Inline measures, context, permissions, data sources
Space B — Metric views (if DDL is supported on the workspace)

Nothing is cleaned up. Run 05_cleanup.py when done.

Docs:
  Genie API:        https://docs.databricks.com/api/workspace/genie
  Genie Context:    https://docs.databricks.com/aws/en/genie/conversation-api
  Metric Views:     https://docs.databricks.com/aws/en/metric-views/
  Permissions API:  https://docs.databricks.com/api/workspace/permissions
"""

import json
import sys
import uuid

sys.path.insert(0, ".")
from config import api_request, execute_sql, init_from_cli, gen_id, CATALOG, SCHEMA

PROFILE = "e2-field"
HOST = "https://e2-demo-field-eng.cloud.databricks.com"
WAREHOUSE_ID = "cd3b290bff658fa3"


def sorted_by_id(items):
    return sorted(items, key=lambda x: x.get("id", ""))


def print_step(num, title):
    print(f"\n{'='*60}")
    print(f"  Step {num}: {title}")
    print(f"{'='*60}")


def print_result(label, value):
    print(f"  {label}: {value}")


# ============================================================================
# Space A: Inline measures, context, permissions
# ============================================================================

def create_space_a():
    """Create Space A with inline measures, full context, and sample questions."""

    sample_questions = sorted_by_id([
        {"id": gen_id(), "question": ["What is our total revenue this quarter?"]},
        {"id": gen_id(), "question": ["Which companies have the most overdue invoices?"]},
        {"id": gen_id(), "question": ["Show me payment trends over the last 6 months."]},
        {"id": gen_id(), "question": ["What is the average invoice amount by company?"]},
    ])

    measures = sorted_by_id([
        {"id": gen_id(), "sql": ["SUM(amount)"], "display_name": "total_revenue"},
        {"id": gen_id(), "sql": ["COUNT(DISTINCT invoice_id)"], "display_name": "invoice_count"},
        {"id": gen_id(), "sql": ["AVG(amount)"], "display_name": "avg_invoice_amount"},
        {"id": gen_id(), "sql": ["SUM(CASE WHEN status = 'OVERDUE' THEN amount ELSE 0 END)"], "display_name": "overdue_amount"},
    ])

    filters = sorted_by_id([
        {"id": gen_id(), "sql": ["invoices.status = 'PAID'"], "display_name": "Paid invoices only"},
        {"id": gen_id(), "sql": ["invoices.invoice_date >= DATE_ADD(CURRENT_DATE(), -90)"], "display_name": "Last 90 days"},
    ])

    expressions = sorted_by_id([
        {
            "id": gen_id(),
            "sql": ["CASE WHEN amount > 10000 THEN 'Large' WHEN amount > 1000 THEN 'Medium' ELSE 'Small' END"],
            "display_name": "invoice_size"
        },
    ])

    example_sqls = sorted_by_id([
        {
            "id": gen_id(),
            "question": ["Total revenue by quarter"],
            "sql": [
                f"SELECT fiscal_quarter, SUM(amount) AS total_revenue ",
                f"FROM {CATALOG}.{SCHEMA}.invoices ",
                "GROUP BY fiscal_quarter ORDER BY fiscal_quarter"
            ]
        },
        {
            "id": gen_id(),
            "question": ["Overdue invoices by company"],
            "sql": [
                f"SELECT a.company_name, COUNT(*) AS overdue_count, SUM(i.amount) AS overdue_total ",
                f"FROM {CATALOG}.{SCHEMA}.invoices i ",
                f"JOIN {CATALOG}.{SCHEMA}.accounts a ON i.company_id = a.account_id ",
                "WHERE i.status = 'OVERDUE' ",
                "GROUP BY a.company_name ORDER BY overdue_total DESC"
            ]
        },
    ])

    payload = {
        "title": "Genie API Demo — Inline Measures",
        "description": "Demo space showing inline measures, context, sample questions, and permissions via the Genie API.",
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps({
            "version": 2,
            "config": {"sample_questions": sample_questions},
            "data_sources": {
                "tables": sorted([
                    {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
                    {
                        "identifier": f"{CATALOG}.{SCHEMA}.invoices",
                        "column_configs": [
                            {"column_name": "amount", "enable_format_assistance": True},
                            {"column_name": "company_id", "enable_entity_matching": True, "enable_format_assistance": True},
                            {"column_name": "status", "enable_entity_matching": True, "enable_format_assistance": True},
                        ]
                    },
                    {"identifier": f"{CATALOG}.{SCHEMA}.payments"},
                ], key=lambda t: t["identifier"])
            },
            "instructions": {
                "text_instructions": [{
                    "id": gen_id(),
                    "content": [
                        "This space answers questions about financial invoices and payments. ",
                        "All monetary values are in USD unless stated otherwise. ",
                        "Fiscal quarters: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec.\n",
                        "\n",
                        "Key joins:\n",
                        "- invoices.company_id = accounts.account_id\n",
                        "- invoices.invoice_id = payments.invoice_id\n",
                    ]
                }],
                "example_question_sqls": example_sqls,
                "sql_snippets": {
                    "filters": filters,
                    "expressions": expressions,
                    "measures": measures,
                }
            }
        })
    }
    return api_request("POST", "/api/2.0/genie/spaces", json_data=payload)


# ============================================================================
# Space B: Metric Views
# ============================================================================

def create_metric_view():
    """Attempt to create a metric view via DDL. Returns result dict (does not raise)."""
    import requests as _requests
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
    import config
    payload = {
        "statement": ddl,
        "warehouse_id": WAREHOUSE_ID,
        "format": "JSON_ARRAY",
        "wait_timeout": "50s",
    }
    headers = {"Authorization": f"Bearer {config.TOKEN}", "Content-Type": "application/json"}
    resp = _requests.post(f"{HOST}/api/2.0/sql/statements/", headers=headers, json=payload)
    try:
        return resp.json()
    except Exception:
        return {"status": {"state": "FAILED", "error": {"message": resp.text[:200]}}}


def create_space_b(with_metric_view=False):
    """Create Space B with metric views (or just tables if DDL isn't supported)."""

    tables = sorted([
        {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
        {"identifier": f"{CATALOG}.{SCHEMA}.invoices"},
        {"identifier": f"{CATALOG}.{SCHEMA}.payments"},
    ], key=lambda t: t["identifier"])

    data_sources = {"tables": tables}
    if with_metric_view:
        data_sources["metric_views"] = [
            {"identifier": f"{CATALOG}.{SCHEMA}.mv_invoice"}
        ]

    title = "Genie API Demo — Metric Views" if with_metric_view else "Genie API Demo — Data Sources"
    desc = (
        "Demo space showing metric views attached via the Genie API."
        if with_metric_view else
        "Demo space showing data source management via the Genie API (metric view DDL not available on this workspace)."
    )

    payload = {
        "title": title,
        "description": desc,
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps({
            "version": 2,
            "data_sources": data_sources,
            "instructions": {
                "text_instructions": [{
                    "id": gen_id(),
                    "content": ["This space uses metric views for governed, reusable financial metrics."]
                }]
            }
        })
    }
    return api_request("POST", "/api/2.0/genie/spaces", json_data=payload)


# ============================================================================
# Main
# ============================================================================

def main():
    print("Genie API Demo")
    print(f"Workspace: {HOST}")
    print(f"Schema: {CATALOG}.{SCHEMA}")
    print("=" * 60)

    import config
    init_from_cli(PROFILE)
    config.WAREHOUSE_ID = WAREHOUSE_ID
    print("Authenticated.\n")

    # ------------------------------------------------------------------
    # Step 1: Create Space A (inline measures + full context)
    # ------------------------------------------------------------------
    print_step(1, "Create Space A — Inline Measures + Context (01_metrics.py, 04_context.py)")
    result_a = create_space_a()
    space_a = result_a["space_id"]
    url_a = f"{HOST}/explore/genie/{space_a}"
    print_result("Space A ID", space_a)
    print_result("Space A URL", url_a)

    # ------------------------------------------------------------------
    # Step 2: Verify Space A context
    # ------------------------------------------------------------------
    print_step(2, "Export & Verify Space A Context (04_context.py)")
    current_a = api_request("GET", f"/api/2.0/genie/spaces/{space_a}",
                            params={"include_serialized_space": "true"})
    config_a = json.loads(current_a["serialized_space"])
    ctx = config_a.get("instructions", {})
    snippets = ctx.get("sql_snippets", {})
    sample_qs = config_a.get("config", {}).get("sample_questions", [])

    print_result("text_instructions", f"{len(ctx.get('text_instructions', []))} entry")
    print_result("example_question_sqls", f"{len(ctx.get('example_question_sqls', []))} queries")
    print_result("sql_snippets.measures", f"{len(snippets.get('measures', []))} measures")
    print_result("sql_snippets.filters", f"{len(snippets.get('filters', []))} filters")
    print_result("sql_snippets.expressions", f"{len(snippets.get('expressions', []))} expressions")
    print_result("config.sample_questions", f"{len(sample_qs)} questions")

    tables_a = config_a.get("data_sources", {}).get("tables", [])
    invoices = next((t for t in tables_a if "invoices" in t["identifier"]), None)
    col_configs = invoices.get("column_configs", []) if invoices else []
    print_result("column_configs (invoices)", f"{len(col_configs)} columns")

    # ------------------------------------------------------------------
    # Step 3: Update Space A — add a measure via PATCH (01_metrics.py)
    # ------------------------------------------------------------------
    print_step(3, "Update Space A — Add Inline Measure via PATCH (01_metrics.py)")
    config_a["instructions"]["sql_snippets"]["measures"].append({
        "id": gen_id(),
        "sql": ["SUM(amount) / NULLIF(COUNT(DISTINCT invoice_id), 0)"],
        "display_name": "revenue_per_invoice"
    })
    config_a["instructions"]["sql_snippets"]["measures"] = sorted_by_id(
        config_a["instructions"]["sql_snippets"]["measures"]
    )
    api_request("PATCH", f"/api/2.0/genie/spaces/{space_a}", json_data={
        "serialized_space": json.dumps(config_a),
    })
    # Verify
    verify_a = api_request("GET", f"/api/2.0/genie/spaces/{space_a}",
                           params={"include_serialized_space": "true"})
    verify_config = json.loads(verify_a["serialized_space"])
    measure_count = len(verify_config["instructions"]["sql_snippets"]["measures"])
    print_result("Measures after update", f"{measure_count} (was 4, added revenue_per_invoice)")

    # ------------------------------------------------------------------
    # Step 4: Update Space A — modify data sources (02_data_sources.py)
    # ------------------------------------------------------------------
    print_step(4, "Update Space A — Remove & Re-add Data Source (02_data_sources.py)")
    # Remove payments
    verify_config["data_sources"]["tables"] = [
        t for t in verify_config["data_sources"]["tables"]
        if t["identifier"] != f"{CATALOG}.{SCHEMA}.payments"
    ]
    api_request("PATCH", f"/api/2.0/genie/spaces/{space_a}", json_data={
        "serialized_space": json.dumps(verify_config),
    })
    check = api_request("GET", f"/api/2.0/genie/spaces/{space_a}",
                        params={"include_serialized_space": "true"})
    check_config = json.loads(check["serialized_space"])
    tables_now = [t["identifier"] for t in check_config["data_sources"]["tables"]]
    print_result("After removing payments", tables_now)

    # Re-add payments
    check_config["data_sources"]["tables"].append({"identifier": f"{CATALOG}.{SCHEMA}.payments"})
    check_config["data_sources"]["tables"] = sorted(
        check_config["data_sources"]["tables"], key=lambda t: t["identifier"]
    )
    api_request("PATCH", f"/api/2.0/genie/spaces/{space_a}", json_data={
        "serialized_space": json.dumps(check_config),
    })
    check2 = api_request("GET", f"/api/2.0/genie/spaces/{space_a}",
                         params={"include_serialized_space": "true"})
    tables_final = [t["identifier"] for t in json.loads(check2["serialized_space"])["data_sources"]["tables"]]
    print_result("After re-adding payments", tables_final)

    # ------------------------------------------------------------------
    # Step 5: Update Space A — permissions (03_permissions.py)
    # ------------------------------------------------------------------
    print_step(5, "Update Space A — Manage Permissions (03_permissions.py)")

    # GET current
    perms = api_request("GET", f"/api/2.0/permissions/genie/{space_a}")
    print("  Current permissions:")
    for entry in perms.get("access_control_list", []):
        principal = entry.get("user_name") or entry.get("group_name") or entry.get("display_name", "unknown")
        levels = [p["permission_level"] for p in entry.get("all_permissions", [])]
        print(f"    {principal}: {', '.join(levels)}")

    # PATCH: grant CAN_RUN to users, CAN_EDIT to admins
    api_request("PATCH", f"/api/2.0/permissions/genie/{space_a}", json_data={
        "access_control_list": [
            {"group_name": "users", "permission_level": "CAN_RUN"},
            {"group_name": "admins", "permission_level": "CAN_EDIT"},
        ]
    })

    perms2 = api_request("GET", f"/api/2.0/permissions/genie/{space_a}")
    print("  Updated permissions:")
    for entry in perms2.get("access_control_list", []):
        principal = entry.get("user_name") or entry.get("group_name") or entry.get("display_name", "unknown")
        levels = [p["permission_level"] for p in entry.get("all_permissions", [])]
        print(f"    {principal}: {', '.join(levels)}")

    # ------------------------------------------------------------------
    # Step 6: Update Space A — append context (04_context.py)
    # ------------------------------------------------------------------
    print_step(6, "Update Space A — Append Text Instruction (04_context.py)")
    latest = api_request("GET", f"/api/2.0/genie/spaces/{space_a}",
                         params={"include_serialized_space": "true"})
    latest_config = json.loads(latest["serialized_space"])
    ti = latest_config["instructions"]["text_instructions"][0]["content"]
    ti.append("\nOverdue invoices are those with status = 'OVERDUE'.")
    api_request("PATCH", f"/api/2.0/genie/spaces/{space_a}", json_data={
        "serialized_space": json.dumps(latest_config),
    })
    print_result("Text instruction lines", f"{len(ti)} (appended overdue definition)")

    # ------------------------------------------------------------------
    # Step 7: Create Space B — Metric Views
    # ------------------------------------------------------------------
    print_step(7, "Create Space B — Metric Views (01_metrics.py Approach B)")

    print("  Attempting metric view DDL...")
    mv_result = create_metric_view()
    mv_state = mv_result.get("status", {}).get("state", "")
    mv_ok = mv_state == "SUCCEEDED"

    if mv_ok:
        print_result("Metric view", f"CREATED — {CATALOG}.{SCHEMA}.mv_invoice")
    else:
        error = mv_result.get("status", {}).get("error", {}).get("message", "")[:100]
        print_result("Metric view", f"SKIPPED — DDL not supported on this workspace ({error})")

    result_b = create_space_b(with_metric_view=mv_ok)
    space_b = result_b["space_id"]
    url_b = f"{HOST}/explore/genie/{space_b}"
    print_result("Space B ID", space_b)
    print_result("Space B URL", url_b)

    # Verify Space B
    current_b = api_request("GET", f"/api/2.0/genie/spaces/{space_b}",
                            params={"include_serialized_space": "true"})
    config_b = json.loads(current_b["serialized_space"])
    tables_b = [t["identifier"] for t in config_b.get("data_sources", {}).get("tables", [])]
    mvs = [m["identifier"] for m in config_b.get("data_sources", {}).get("metric_views", [])]
    print_result("Tables", tables_b)
    print_result("Metric views", mvs if mvs else "none (DDL not available)")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  DEMO COMPLETE — Both spaces are live")
    print("=" * 60)
    print(f"\n  Space A (Inline):       {url_a}")
    print(f"  Space B (Metric Views): {url_b}")
    print(f"\n  To clean up: python3 05_cleanup.py")
    print()


if __name__ == "__main__":
    main()
