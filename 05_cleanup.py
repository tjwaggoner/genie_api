"""
Example 5: Clean Up Genie Spaces and Test Artifacts

Deletes Genie Spaces and optionally drops tables/views created by the examples.

API Endpoints:
  GET    /api/2.0/genie/spaces                    - List spaces
  DELETE /api/2.0/genie/spaces/{space_id}          - Delete a space
  POST   /api/2.0/sql/statements/                  - Execute SQL (DROP TABLE/VIEW)

Docs:
  List Spaces:          https://docs.databricks.com/api/workspace/genie/listspaces
  Statement Execution:  https://docs.databricks.com/api/workspace/statementexecution
"""

import json
from config import api_request, execute_sql, CATALOG, SCHEMA


def list_spaces():
    """List all Genie Spaces the caller has access to."""
    return api_request("GET", "/api/2.0/genie/spaces")


def delete_space(space_id):
    """Delete a Genie Space by ID."""
    return api_request("DELETE", f"/api/2.0/genie/spaces/{space_id}")


def find_test_spaces():
    """Find spaces created by the genie_api examples."""
    spaces = list_spaces()
    test_keywords = [
        "Test Metrics Space",
        "Test Data Sources Space",
        "Test Permissions Space",
        "Test Context Space",
        "Finance Metrics Space",
        "Finance Data Space",
        "Finance Analytics Space",
        "Genie API Examples",
        "Genie API Demo",
    ]
    matches = []
    for s in spaces.get("spaces", []):
        title = s.get("title", "")
        if any(kw.lower() in title.lower() for kw in test_keywords):
            matches.append(s)
    return matches


def drop_test_tables():
    """Drop tables and views created by the examples."""
    objects = [
        f"{CATALOG}.{SCHEMA}.invoices",
        f"{CATALOG}.{SCHEMA}.payments",
        f"{CATALOG}.{SCHEMA}.accounts",
        f"{CATALOG}.{SCHEMA}.mv_invoice",
    ]
    results = []
    for obj in objects:
        r = execute_sql(f"DROP TABLE IF EXISTS {obj}")
        state = r.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            r = execute_sql(f"DROP VIEW IF EXISTS {obj}")
            state = r.get("status", {}).get("state", "")
        results.append((obj, state))
    return results


def drop_test_schema():
    """Drop the test schema if empty."""
    r = execute_sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA}")
    return r.get("status", {}).get("state", "")


if __name__ == "__main__":
    import sys
    from config import init_from_cli
    init_from_cli()

    print("=== Finding Test Genie Spaces ===")
    test_spaces = find_test_spaces()

    if not test_spaces:
        print("  No test spaces found.")
    else:
        for s in test_spaces:
            print(f"  {s['space_id']}: {s['title']}")

        print(f"\nFound {len(test_spaces)} space(s) to delete.")
        confirm = input("Delete these spaces? [y/N] ").strip().lower()
        if confirm == "y":
            for s in test_spaces:
                delete_space(s["space_id"])
                print(f"  Deleted: {s['title']}")
        else:
            print("  Skipped space deletion.")

    if "--tables" in sys.argv:
        print("\n=== Dropping Test Tables/Views ===")
        results = drop_test_tables()
        for obj, state in results:
            print(f"  {obj}: {state}")

    if "--schema" in sys.argv:
        print("\n=== Dropping Test Schema ===")
        state = drop_test_schema()
        print(f"  {CATALOG}.{SCHEMA}: {state}")

    if "--tables" not in sys.argv and "--schema" not in sys.argv:
        print("\nTip: Use --tables to also drop test tables, --schema to drop the schema.")
