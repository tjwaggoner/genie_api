"""
Example 2: Add/Update Data Sources in a Genie Space

Demonstrates the full CRUD lifecycle for managing data sources
(tables, views, metric views) attached to a Genie Space.

API Endpoints:
  POST  /api/2.0/genie/spaces                                         - Create
  GET   /api/2.0/genie/spaces                                         - List
  GET   /api/2.0/genie/spaces/{space_id}?include_serialized_space=true - Export
  PATCH /api/2.0/genie/spaces/{space_id}                               - Update
  DELETE /api/2.0/genie/spaces/{space_id}                              - Delete

Constraints:
  - Up to 30 tables/views per space
  - All identifiers use three-level namespace: catalog.schema.table
  - Tables must be sorted alphabetically by identifier in serialized_space

Docs:
  Genie API:        https://docs.databricks.com/api/workspace/genie
  Create Space:     https://docs.databricks.com/api/workspace/genie/createspace
  Get Space:        https://docs.databricks.com/api/workspace/genie/getspace
  List Spaces:      https://docs.databricks.com/api/workspace/genie/listspaces
  Genie Context:    https://docs.databricks.com/aws/en/genie/conversation-api
"""

import json
from config import api_request, gen_id, CATALOG, SCHEMA


def list_spaces():
    """
    List all Genie Spaces the caller has access to.

    GET /api/2.0/genie/spaces
    Docs: https://docs.databricks.com/api/workspace/genie/listspaces
    """
    return api_request("GET", "/api/2.0/genie/spaces")


def get_space_config(space_id):
    """
    Export the full serialized space config including data sources.

    GET /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    Docs: https://docs.databricks.com/api/workspace/genie/getspace
    """
    return api_request(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        params={"include_serialized_space": "true"},
    )


def create_space_with_data_sources(warehouse_id):
    """
    Create a new Genie Space with initial data sources.
    Tables must be sorted alphabetically by identifier.

    POST /api/2.0/genie/spaces
    Docs: https://docs.databricks.com/api/workspace/genie/createspace
    """
    tables = sorted([
        {"identifier": f"{CATALOG}.{SCHEMA}.invoices"},
        {"identifier": f"{CATALOG}.{SCHEMA}.payments"},
        {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
    ], key=lambda t: t["identifier"])

    payload = {
        "title": "Finance Data Space",
        "description": "Genie space for financial data exploration",
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps({
            "version": 2,
            "data_sources": {"tables": tables},
            "instructions": {
                "text_instructions": [
                    {
                        "id": gen_id(),
                        "content": ["This space answers questions about invoices, payments, and accounts."]
                    }
                ]
            }
        })
    }
    return api_request("POST", "/api/2.0/genie/spaces", json_data=payload)


def add_data_source(space_id, table_identifier):
    """
    Add a new table or view to an existing Genie Space.

    GET   /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    PATCH /api/2.0/genie/spaces/{space_id}
    """
    current = get_space_config(space_id)
    config = json.loads(current["serialized_space"])

    config.setdefault("data_sources", {}).setdefault("tables", [])
    config["data_sources"]["tables"].append({"identifier": table_identifier})
    config["data_sources"]["tables"] = sorted(
        config["data_sources"]["tables"], key=lambda t: t["identifier"]
    )

    return api_request("PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data={
        "serialized_space": json.dumps(config),
    })


def remove_data_source(space_id, table_identifier):
    """
    Remove a table or view from an existing Genie Space.

    GET   /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    PATCH /api/2.0/genie/spaces/{space_id}
    """
    current = get_space_config(space_id)
    config = json.loads(current["serialized_space"])

    config["data_sources"]["tables"] = [
        t for t in config.get("data_sources", {}).get("tables", [])
        if t["identifier"] != table_identifier
    ]

    return api_request("PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data={
        "serialized_space": json.dumps(config),
    })


def replace_all_data_sources(space_id, table_identifiers):
    """
    Replace all data sources on a Genie Space.

    GET   /api/2.0/genie/spaces/{space_id}?include_serialized_space=true
    PATCH /api/2.0/genie/spaces/{space_id}
    """
    current = get_space_config(space_id)
    config = json.loads(current["serialized_space"])

    config["data_sources"] = {
        "tables": sorted(
            [{"identifier": tid} for tid in table_identifiers],
            key=lambda t: t["identifier"]
        )
    }

    return api_request("PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data={
        "serialized_space": json.dumps(config),
    })


if __name__ == "__main__":
    from config import init_from_cli, WAREHOUSE_ID
    init_from_cli()

    wh = WAREHOUSE_ID or "cd3b290bff658fa3"

    print("=== List Spaces ===")
    spaces = list_spaces()
    for s in spaces.get("spaces", [])[:5]:
        print(f"  {s['space_id']}: {s['title']}")

    print("\n=== Create Space with Data Sources ===")
    result = create_space_with_data_sources(wh)
    new_space_id = result["space_id"]
    print(f"Space created: {new_space_id}")

    print("\n=== Remove Data Source ===")
    remove_data_source(new_space_id, f"{CATALOG}.{SCHEMA}.accounts")
    print(f"Removed: {CATALOG}.{SCHEMA}.accounts")

    print("\n=== Current Config ===")
    config = get_space_config(new_space_id)
    parsed = json.loads(config["serialized_space"])
    for t in parsed.get("data_sources", {}).get("tables", []):
        print(f"  {t['identifier']}")
