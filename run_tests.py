"""
Integration test runner for all Genie API examples.
Executes each example against a live Databricks workspace.
"""

import json
import subprocess
import sys
import uuid

# --- Config ---
PROFILE = "e2-field"
HOST = "https://e2-demo-field-eng.cloud.databricks.com"
WAREHOUSE_ID = "cd3b290bff658fa3"
CATALOG = "waggoner"
SCHEMA = "finance"


def gen_id():
    return uuid.uuid4().hex


def get_token():
    result = subprocess.run(
        ["databricks", "auth", "token", f"--profile={PROFILE}"],
        capture_output=True, text=True,
    )
    return json.loads(result.stdout)["access_token"]


def api_request(token, method, path, json_data=None, params=None):
    import requests
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{HOST}{path}"
    resp = requests.request(method, url, headers=headers, json=json_data, params=params)
    if not resp.ok:
        print(f"    DEBUG: {resp.status_code} {resp.text[:300]}")
    resp.raise_for_status()
    return resp.json() if resp.text else {}


def execute_sql(token, statement):
    payload = {
        "statement": statement,
        "warehouse_id": WAREHOUSE_ID,
        "format": "JSON_ARRAY",
        "wait_timeout": "50s",
    }
    return api_request(token, "POST", "/api/2.0/sql/statements/", json_data=payload)


def create_test_space(token, title, extra_instructions=None, extra_config=None):
    """Helper to create a minimal test space with the v2 schema."""
    ss = {
        "version": 2,
        "data_sources": {
            "tables": [
                {"identifier": f"{CATALOG}.{SCHEMA}.invoices"},
            ]
        },
        "instructions": {
            "text_instructions": [
                {"id": gen_id(), "content": ["Test space."]}
            ]
        }
    }
    if extra_instructions:
        ss["instructions"].update(extra_instructions)
    if extra_config:
        ss["config"] = extra_config

    ss = sort_serialized_space(ss)
    payload = {
        "title": title,
        "description": "Automated test",
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(ss),
    }
    return api_request(token, "POST", "/api/2.0/genie/spaces", json_data=payload)


def delete_space(token, space_id):
    api_request(token, "DELETE", f"/api/2.0/genie/spaces/{space_id}")


def get_space(token, space_id):
    return api_request(token, "GET", f"/api/2.0/genie/spaces/{space_id}",
                       params={"include_serialized_space": "true"})


def sort_serialized_space(config):
    """Sort all ID-bearing lists in serialized_space as the API requires."""
    if "data_sources" in config and "tables" in config["data_sources"]:
        config["data_sources"]["tables"] = sorted(
            config["data_sources"]["tables"], key=lambda t: t["identifier"]
        )
    snippets = config.get("instructions", {}).get("sql_snippets", {})
    for key in ("measures", "filters", "expressions"):
        if key in snippets:
            snippets[key] = sorted(snippets[key], key=lambda x: x.get("id", ""))
    eq = config.get("instructions", {}).get("example_question_sqls", [])
    if eq:
        config["instructions"]["example_question_sqls"] = sorted(eq, key=lambda x: x.get("id", ""))
    ti = config.get("instructions", {}).get("text_instructions", [])
    if ti:
        config["instructions"]["text_instructions"] = sorted(ti, key=lambda x: x.get("id", ""))
    sq = config.get("config", {}).get("sample_questions", [])
    if sq:
        config["config"]["sample_questions"] = sorted(sq, key=lambda x: x.get("id", ""))
    return config


def update_space(token, space_id, current, config):
    """Update a space via PATCH. All lists must be sorted by their ID/identifier."""
    config = sort_serialized_space(config)
    payload = {
        "serialized_space": json.dumps(config),
    }
    return api_request(token, "PATCH", f"/api/2.0/genie/spaces/{space_id}", json_data=payload)


# ============================================================================
# Test 1: Metrics
# ============================================================================

def test_01_metrics(token):
    print("=" * 60)
    print("TEST 1: Metrics (01_metrics.py)")
    print("=" * 60)

    # --- A: Create space with inline measures ---
    print("\n[A] Creating space with inline measures...")
    result = create_test_space(token, "Test Metrics Space", extra_instructions={
        "sql_snippets": {
            "measures": [
                {"id": gen_id(), "sql": ["SUM(amount)"], "display_name": "total_revenue"},
                {"id": gen_id(), "sql": ["COUNT(DISTINCT invoice_id)"], "display_name": "invoice_count"},
            ]
        }
    })
    space_id = result["space_id"]
    print(f"    PASS - Space created: {space_id}")

    # --- Update inline measures ---
    print("[A] Updating inline measures...")
    current = get_space(token, space_id)
    config = json.loads(current["serialized_space"])
    config["instructions"]["sql_snippets"]["measures"].append(
        {"id": gen_id(), "sql": ["AVG(amount)"], "display_name": "avg_amount"}
    )
    update_space(token, space_id, current, config)
    print("    PASS - Measures updated")

    # --- Verify ---
    print("[A] Verifying measures...")
    verify = get_space(token, space_id)
    vc = json.loads(verify["serialized_space"])
    measures = vc.get("instructions", {}).get("sql_snippets", {}).get("measures", [])
    assert len(measures) == 3, f"Expected 3 measures, got {len(measures)}"
    print(f"    PASS - Verified {len(measures)} measures")

    # --- B: Metric view via DDL ---
    # Note: Metric Views DDL requires the workspace to have the feature enabled (preview).
    # The DDL syntax is: CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$ FROM ...
    print("\n[B] Creating metric view via DDL...")
    mv_ddl = (
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
        f"  measures:\n"
        f"    - name: Total Revenue\n"
        f"      expr: SUM(amount)\n"
        f"\n"
        f"    - name: Invoice Count\n"
        f"      expr: COUNT(DISTINCT invoice_id)\n"
        f"$$;"
    )
    mv_result = execute_sql(token, mv_ddl)
    mv_state = mv_result.get("status", {}).get("state", "")
    mv_succeeded = mv_state == "SUCCEEDED"
    if mv_succeeded:
        print(f"    PASS - Metric view created: {CATALOG}.{SCHEMA}.mv_invoice")
    else:
        error = mv_result.get("status", {}).get("error", {}).get("message", "unknown")[:120]
        print(f"    SKIP - Metric view DDL not supported on this workspace ({error})")

    # --- Attach metric view (only if DDL succeeded) ---
    if mv_succeeded:
        print("[B] Attaching metric view to space...")
        current = get_space(token, space_id)
        config = json.loads(current["serialized_space"])
        config.setdefault("data_sources", {}).setdefault("metric_views", [])
        config["data_sources"]["metric_views"].append(
            {"identifier": f"{CATALOG}.{SCHEMA}.mv_invoice"}
        )
        update_space(token, space_id, current, config)
        print("    PASS - Metric view attached")
    else:
        print("[B] Skipping metric view attach (DDL did not succeed)")

    created_spaces.append(space_id)
    return True


# ============================================================================
# Test 2: Data Sources
# ============================================================================

def test_02_data_sources(token):
    print("\n" + "=" * 60)
    print("TEST 2: Data Sources (02_data_sources.py)")
    print("=" * 60)

    # --- List ---
    print("\n[List] Listing Genie Spaces...")
    spaces = api_request(token, "GET", "/api/2.0/genie/spaces")
    count = len(spaces.get("spaces", []))
    print(f"    PASS - Found {count} existing spaces")

    # --- Create with 3 tables (sorted alphabetically) ---
    print("[Create] Creating space with 3 data sources...")
    ss = {
        "version": 2,
        "data_sources": {
            "tables": [
                {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
                {"identifier": f"{CATALOG}.{SCHEMA}.invoices"},
                {"identifier": f"{CATALOG}.{SCHEMA}.payments"},
            ]
        },
        "instructions": {
            "text_instructions": [{"id": gen_id(), "content": ["Test."]}]
        }
    }
    payload = {
        "title": "Test Data Sources Space",
        "description": "Testing data source CRUD",
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(ss),
    }
    result = api_request(token, "POST", "/api/2.0/genie/spaces", json_data=payload)
    space_id = result["space_id"]
    print(f"    PASS - Space created: {space_id}")

    # --- Verify initial ---
    print("[Verify] Checking initial data sources...")
    current = get_space(token, space_id)
    config = json.loads(current["serialized_space"])
    tables = config.get("data_sources", {}).get("tables", [])
    assert len(tables) == 3, f"Expected 3, got {len(tables)}"
    print(f"    PASS - {len(tables)} data sources confirmed")

    # --- Update: remove accounts ---
    print("[Remove] Removing accounts table...")
    config["data_sources"]["tables"] = [
        t for t in config["data_sources"]["tables"]
        if t["identifier"] != f"{CATALOG}.{SCHEMA}.accounts"
    ]
    update_space(token, space_id, current, config)

    verify = get_space(token, space_id)
    vc = json.loads(verify["serialized_space"])
    remaining = [t["identifier"] for t in vc.get("data_sources", {}).get("tables", [])]
    assert f"{CATALOG}.{SCHEMA}.accounts" not in remaining
    print(f"    PASS - Remaining: {remaining}")

    # --- Replace all ---
    print("[Replace] Replacing all data sources...")
    current2 = get_space(token, space_id)
    config2 = json.loads(current2["serialized_space"])
    config2["data_sources"]["tables"] = [
        {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
        {"identifier": f"{CATALOG}.{SCHEMA}.invoices"},
    ]
    update_space(token, space_id, current2, config2)
    verify2 = get_space(token, space_id)
    vc2 = json.loads(verify2["serialized_space"])
    final = [t["identifier"] for t in vc2.get("data_sources", {}).get("tables", [])]
    assert len(final) == 2
    print(f"    PASS - Replaced: {final}")

    created_spaces.append(space_id)
    return True


# ============================================================================
# Test 3: Permissions
# ============================================================================

def test_03_permissions(token):
    print("\n" + "=" * 60)
    print("TEST 3: Permissions (03_permissions.py)")
    print("=" * 60)

    # --- Create test space ---
    print("\n[Setup] Creating test space...")
    result = create_test_space(token, "Test Permissions Space")
    space_id = result["space_id"]
    print(f"    PASS - Space created: {space_id}")

    # --- GET permissions ---
    print("[GET] Fetching permissions...")
    perms = api_request(token, "GET", f"/api/2.0/permissions/genie/{space_id}")
    acl = perms.get("access_control_list", [])
    print(f"    PASS - {len(acl)} permission entries")
    for entry in acl:
        principal = entry.get("user_name") or entry.get("group_name") or "unknown"
        levels = [p["permission_level"] for p in entry.get("all_permissions", [])]
        print(f"           {principal}: {', '.join(levels)}")

    # --- PATCH: add group (use permission_level directly, not nested in all_permissions) ---
    print("[PATCH] Granting CAN_RUN to users group...")
    api_request(token, "PATCH", f"/api/2.0/permissions/genie/{space_id}", json_data={
        "access_control_list": [
            {"group_name": "users", "permission_level": "CAN_RUN"}
        ]
    })
    print("    PASS - CAN_RUN granted")

    # --- Verify ---
    print("[Verify] Checking updated permissions...")
    perms2 = api_request(token, "GET", f"/api/2.0/permissions/genie/{space_id}")
    acl2 = perms2.get("access_control_list", [])
    groups = [e.get("group_name") for e in acl2 if e.get("group_name")]
    assert "users" in groups, "users group should be present"
    print(f"    PASS - {len(acl2)} entries, 'users' group confirmed")

    # --- PATCH: add another level ---
    print("[PATCH] Granting CAN_EDIT to admins group...")
    api_request(token, "PATCH", f"/api/2.0/permissions/genie/{space_id}", json_data={
        "access_control_list": [
            {"group_name": "admins", "permission_level": "CAN_EDIT"}
        ]
    })
    print("    PASS - CAN_EDIT granted")

    # --- Final summary ---
    print("[Final] Permission summary:")
    perms3 = api_request(token, "GET", f"/api/2.0/permissions/genie/{space_id}")
    for entry in perms3.get("access_control_list", []):
        principal = entry.get("user_name") or entry.get("group_name") or "unknown"
        levels = [p["permission_level"] for p in entry.get("all_permissions", [])]
        print(f"           {principal}: {', '.join(levels)}")

    created_spaces.append(space_id)
    return True


# ============================================================================
# Test 4: Context & Docs
# ============================================================================

def test_04_context(token):
    print("\n" + "=" * 60)
    print("TEST 4: Context & Docs (04_context.py)")
    print("=" * 60)

    # --- Create with full context ---
    print("\n[Create] Creating space with comprehensive context...")
    ss = {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": gen_id(), "question": ["What is our total revenue this quarter?"]},
                {"id": gen_id(), "question": ["Which companies have the most overdue invoices?"]},
            ]
        },
        "data_sources": {
            "tables": [
                {"identifier": f"{CATALOG}.{SCHEMA}.accounts"},
                {
                    "identifier": f"{CATALOG}.{SCHEMA}.invoices",
                    "column_configs": [
                        {"column_name": "amount", "enable_format_assistance": True},
                        {"column_name": "company_id", "enable_entity_matching": True, "enable_format_assistance": True},
                    ]
                },
                {"identifier": f"{CATALOG}.{SCHEMA}.payments"},
            ]
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": gen_id(),
                    "content": [
                        "This space answers questions about financial invoices. ",
                        "All monetary values are in USD."
                    ]
                }
            ],
            "example_question_sqls": [
                {
                    "id": gen_id(),
                    "question": ["Total revenue by quarter"],
                    "sql": [
                        f"SELECT fiscal_quarter, SUM(amount) AS total_revenue ",
                        f"FROM {CATALOG}.{SCHEMA}.invoices ",
                        "GROUP BY fiscal_quarter ORDER BY fiscal_quarter"
                    ]
                },
            ],
            "sql_snippets": {
                "filters": [
                    {"id": gen_id(), "sql": ["invoices.status = 'PAID'"], "display_name": "Paid invoices only"}
                ],
                "expressions": [
                    {
                        "id": gen_id(),
                        "sql": ["CASE WHEN amount > 10000 THEN 'Large' ELSE 'Small' END"],
                        "display_name": "invoice_size"
                    }
                ],
                "measures": [
                    {"id": gen_id(), "sql": ["SUM(amount)"], "display_name": "total_revenue"},
                    {"id": gen_id(), "sql": ["COUNT(DISTINCT invoice_id)"], "display_name": "invoice_count"},
                ]
            }
        }
    }
    ss = sort_serialized_space(ss)
    payload = {
        "title": "Test Context Space",
        "description": "Testing all context types",
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(ss),
    }
    result = api_request(token, "POST", "/api/2.0/genie/spaces", json_data=payload)
    space_id = result["space_id"]
    print(f"    PASS - Space created: {space_id}")

    # --- Export ---
    print("[Export] Exporting context...")
    current = get_space(token, space_id)
    config = json.loads(current["serialized_space"])
    ctx = config.get("instructions", {})
    print(f"    PASS - Instruction keys: {list(ctx.keys())}")

    # --- Verify all types ---
    print("[Verify] Checking all context types...")
    assert "text_instructions" in ctx
    assert "example_question_sqls" in ctx
    assert "sql_snippets" in ctx
    snippets = ctx["sql_snippets"]
    assert "measures" in snippets
    assert "filters" in snippets
    assert "expressions" in snippets
    sample_qs = config.get("config", {}).get("sample_questions", [])

    print(f"    PASS - All context types present:")
    print(f"           text_instructions: {len(ctx['text_instructions'])} entries")
    print(f"           example_question_sqls: {len(ctx['example_question_sqls'])} queries")
    print(f"           sql_snippets.measures: {len(snippets['measures'])} measures")
    print(f"           sql_snippets.filters: {len(snippets['filters'])} filters")
    print(f"           sql_snippets.expressions: {len(snippets['expressions'])} expressions")
    print(f"           config.sample_questions: {len(sample_qs)} questions")

    # --- Column configs ---
    tables = config.get("data_sources", {}).get("tables", [])
    invoices_table = next((t for t in tables if "invoices" in t["identifier"]), None)
    col_configs = invoices_table.get("column_configs", []) if invoices_table else []
    print(f"           column_configs on invoices: {len(col_configs)} columns")

    # --- Update context (text_instructions allows only 1 item, so append to its content array) ---
    print("[Update] Appending to text instruction content...")
    ctx["text_instructions"][0]["content"].append("\nFiscal year starts in January.")
    update_space(token, space_id, current, config)
    print("    PASS - Context updated")

    # --- Verify update ---
    print("[Verify] Checking updated context...")
    updated = get_space(token, space_id)
    uc = json.loads(updated["serialized_space"])
    ti = uc.get("instructions", {}).get("text_instructions", [])
    content = ti[0]["content"]
    assert any("Fiscal year" in c for c in content), "Updated content not found"
    print(f"    PASS - text_instructions content: {len(content)} lines")

    created_spaces.append(space_id)
    return True


# ============================================================================
# Main
# ============================================================================

created_spaces = []

def main():
    print("Genie API Integration Tests")
    print(f"Workspace: {HOST}")
    print(f"Schema: {CATALOG}.{SCHEMA}")
    print("=" * 60)

    token = get_token()
    print(f"Token acquired: {token[:20]}...\n")

    results = {}
    for name, test_fn in [
        ("01_metrics", test_01_metrics),
        ("02_data_sources", test_02_data_sources),
        ("03_permissions", test_03_permissions),
        ("04_context", test_04_context),
    ]:
        try:
            test_fn(token)
            results[name] = "PASS"
        except Exception as e:
            results[name] = f"FAIL: {e}"
            print(f"    FAIL - {e}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, status in results.items():
        icon = "PASS" if status == "PASS" else "FAIL"
        print(f"  [{icon}] {name}: {status}")

    # --- Cleanup via 05_cleanup.py ---
    if created_spaces:
        print("\n" + "=" * 60)
        print("CLEANUP")
        print("=" * 60)
        for sid in created_spaces:
            try:
                delete_space(token, sid)
                print(f"  Deleted: {sid}")
            except Exception as e:
                print(f"  Failed to delete {sid}: {e}")

    all_passed = all(v == "PASS" for v in results.values())
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
