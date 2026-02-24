"""
Shared configuration for Genie API examples.

Set DATABRICKS_HOST and DATABRICKS_TOKEN as environment variables,
or use init_from_cli() to pull credentials from the Databricks CLI.

Docs:
  Genie API:             https://docs.databricks.com/api/workspace/genie
  Statement Execution:   https://docs.databricks.com/api/workspace/statementexecution
  Permissions API:       https://docs.databricks.com/api/workspace/permissions
"""

import json
import os
import subprocess
import requests

HOST = os.environ.get("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

CATALOG = "waggoner"
SCHEMA = "finance"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def init_from_cli(profile="e2-field"):
    """Initialize HOST/TOKEN/WAREHOUSE_ID from Databricks CLI profile."""
    global HOST, TOKEN, WAREHOUSE_ID
    result = subprocess.run(
        ["databricks", "auth", "token", f"--profile={profile}"],
        capture_output=True, text=True,
    )
    token_data = json.loads(result.stdout)
    TOKEN = token_data["access_token"]


def _headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }


def api_request(method, path, json_data=None, params=None):
    """Make an authenticated request to the Databricks REST API."""
    url = f"{HOST}{path}"
    resp = requests.request(method, url, headers=_headers(), json=json_data, params=params)
    resp.raise_for_status()
    return resp.json() if resp.text else {}


def execute_sql(statement, warehouse_id=None):
    """Execute a SQL statement via the Statement Execution API.

    Docs: https://docs.databricks.com/api/workspace/statementexecution/executestatement
    """
    payload = {
        "statement": statement,
        "warehouse_id": warehouse_id or WAREHOUSE_ID,
        "format": "JSON_ARRAY",
        "wait_timeout": "50s",
    }
    return api_request("POST", "/api/2.0/sql/statements/", json_data=payload)


def gen_id():
    """Generate a 32-character hex ID for serialized_space objects."""
    import uuid
    return uuid.uuid4().hex
