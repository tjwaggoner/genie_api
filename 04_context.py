"""
Example 4: List/Add/Update Context and Docs in a Genie Space

Manages all context source types in the v2 serialized_space format.

API Endpoints:
  POST   /api/2.0/genie/spaces                                         - Create with context
  GET    /api/2.0/genie/spaces/{space_id}?include_serialized_space=true - Export (requires CAN_EDIT)
  PATCH  /api/2.0/genie/spaces/{space_id}                              - Update context

Context types available in serialized_space:
  - text_instructions: Free-text domain knowledge (1 entry max, content is an array of strings)
  - example_question_sqls: Static or parameterized SQL queries (parameterized get "Trusted" badge)
  - sql_snippets.filters: Pre-defined WHERE clause conditions
  - sql_snippets.expressions: Calculated columns / dimensions
  - sql_snippets.measures: Aggregation-based KPIs
  - config.sample_questions: Example questions shown in the UI
  - column_configs: Per-table column synonyms, format hints, entity matching (on data_sources.tables)

Sorting: All ID-bearing lists must be sorted by their `id` field.

Docs:
  Genie API:        https://docs.databricks.com/api/workspace/genie
  Create Space:     https://docs.databricks.com/api/workspace/genie/createspace
  Get Space:        https://docs.databricks.com/api/workspace/genie/getspace
  Genie Context:    https://docs.databricks.com/aws/en/genie/conversation-api
  Genie Setup:      https://docs.databricks.com/aws/en/genie/set-up
"""

import json
from config import api_request, gen_id, CATALOG, SCHEMA


def get_context(space_id):
    """
    Export the full serialized_space from a Genie Space.

    GET /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    Docs: https://docs.databricks.com/api/workspace/genie/getspace
    """
    result = api_request(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        params={"include_serialized_space": "true"},
    )
    return json.loads(result["serialized_space"])


def update_context(space_id, serialized_space_dict):
    """
    Update the serialized_space on a Genie Space via PATCH.

    PATCH /api/2.0/genie/spaces/{space_id}
    Docs: https://docs.databricks.com/aws/en/genie/conversation-api
    """
    return api_request("PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data={
        "serialized_space": json.dumps(serialized_space_dict),
    })


def _sorted_by_id(items):
    """Sort a list of dicts by their 'id' field (API requirement)."""
    return sorted(items, key=lambda x: x.get("id", ""))


def build_full_context_payload(warehouse_id):
    """Build a comprehensive space payload demonstrating all context source types."""

    sample_questions = _sorted_by_id([
        {"id": gen_id(), "question": ["What is our total revenue this quarter?"]},
        {"id": gen_id(), "question": ["Which companies have the most overdue invoices?"]},
        {"id": gen_id(), "question": ["Show me payment trends over the last 6 months."]},
        {"id": gen_id(), "question": ["What is the average invoice amount by company?"]},
    ])

    text_instructions = [{
        "id": gen_id(),
        "content": [
            "This space answers questions about financial invoices and payments. ",
            "All monetary values are in USD unless stated otherwise. ",
            "Fiscal quarters follow the standard calendar: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec.\n",
            "\n",
            "Key joins:\n",
            "- invoices.company_id = accounts.account_id\n",
            "- invoices.invoice_id = payments.invoice_id\n",
        ]
    }]

    example_sqls = _sorted_by_id([
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

    filters = _sorted_by_id([
        {"id": gen_id(), "sql": ["invoices.status = 'PAID'"], "display_name": "Paid invoices only"},
        {"id": gen_id(), "sql": ["invoices.invoice_date >= DATE_ADD(CURRENT_DATE(), -90)"], "display_name": "Last 90 days"},
    ])

    expressions = _sorted_by_id([
        {
            "id": gen_id(),
            "sql": ["CASE WHEN amount > 10000 THEN 'Large' WHEN amount > 1000 THEN 'Medium' ELSE 'Small' END"],
            "display_name": "invoice_size"
        }
    ])

    measures = _sorted_by_id([
        {"id": gen_id(), "sql": ["SUM(amount)"], "display_name": "total_revenue"},
        {"id": gen_id(), "sql": ["COUNT(DISTINCT invoice_id)"], "display_name": "invoice_count"},
        {"id": gen_id(), "sql": ["SUM(CASE WHEN status = 'OVERDUE' THEN amount ELSE 0 END)"], "display_name": "overdue_amount"},
    ])

    return {
        "title": "Finance Analytics Space",
        "description": "Comprehensive financial analytics with full context",
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps({
            "version": 2,
            "config": {"sample_questions": sample_questions},
            "data_sources": {
                "tables": sorted([
                    {
                        "identifier": f"{CATALOG}.{SCHEMA}.invoices",
                        "column_configs": [
                            {"column_name": "amount", "enable_format_assistance": True},
                            {"column_name": "company_id", "enable_entity_matching": True, "enable_format_assistance": True},
                            {"column_name": "status", "enable_entity_matching": True, "enable_format_assistance": True},
                        ]
                    },
                    {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
                    {"identifier": f"{CATALOG}.{SCHEMA}.payments"},
                ], key=lambda t: t["identifier"])
            },
            "instructions": {
                "text_instructions": text_instructions,
                "example_question_sqls": example_sqls,
                "sql_snippets": {
                    "filters": filters,
                    "expressions": expressions,
                    "measures": measures,
                }
            }
        })
    }


def create_space_with_full_context(warehouse_id):
    """
    Create a new Genie Space with all context types populated.

    POST /api/2.0/genie/spaces
    Docs: https://docs.databricks.com/api/workspace/genie/createspace
    """
    payload = build_full_context_payload(warehouse_id)
    return api_request("POST", "/api/2.0/genie/spaces", json_data=payload)


if __name__ == "__main__":
    from config import init_from_cli, WAREHOUSE_ID
    init_from_cli()

    wh = WAREHOUSE_ID or "cd3b290bff658fa3"

    print("=== Create Space with Full Context ===")
    result = create_space_with_full_context(wh)
    new_space_id = result["space_id"]
    print(f"Space created: {new_space_id}")

    print("\n=== Export Context ===")
    config = get_context(new_space_id)
    ctx = config.get("instructions", {})
    print(f"Context keys: {list(ctx.keys())}")

    print("\n=== Update Context ===")
    config["instructions"]["text_instructions"][0]["content"].append(
        "\nAdditional rule: Fiscal year starts in January."
    )
    update_context(new_space_id, config)
    print("Context updated.")
