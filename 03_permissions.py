"""
Example 3: Add/Edit Roles and Permissions on a Genie Space

Uses the Workspace Permissions API with object type `genie` (not `genie-spaces`).

API Endpoints:
  GET   /api/2.0/permissions/genie/{space_id}                  - View permissions
  GET   /api/2.0/permissions/genie/{space_id}/permissionLevels - List valid levels
  PATCH /api/2.0/permissions/genie/{space_id}                  - Add/update (additive)
  PUT   /api/2.0/permissions/genie/{space_id}                  - Replace all (destructive)

Available permission levels:
  - CAN_READ:   Ask questions, view responses, provide feedback
  - CAN_RUN:    Same as CAN_READ
  - CAN_EDIT:   + add/edit instructions, sample questions, tables, context
  - CAN_MANAGE: + monitor usage, modify permissions, delete space, view all conversations

PATCH format uses `permission_level` directly on the ACL entry (not nested in all_permissions).

Docs:
  Permissions API:           https://docs.databricks.com/api/workspace/permissions
  Get Object Permissions:    https://docs.databricks.com/api/workspace/permissions/get
  Set Object Permissions:    https://docs.databricks.com/api/workspace/permissions/set
  Update Object Permissions: https://docs.databricks.com/api/workspace/permissions/update
  Genie Space Setup:         https://docs.databricks.com/aws/en/genie/set-up
"""

from config import api_request

SPACE_ID = "<your-space-id>"


def get_permission_levels(space_id):
    """
    List all valid permission levels for a Genie Space.

    GET /api/2.0/permissions/genie/{space_id}/permissionLevels
    Docs: https://docs.databricks.com/api/workspace/permissions/getpermissionlevels
    """
    return api_request("GET", f"/api/2.0/permissions/genie/{space_id}/permissionLevels")


def get_permissions(space_id):
    """
    Get current permissions for a Genie Space.

    GET /api/2.0/permissions/genie/{space_id}
    Docs: https://docs.databricks.com/api/workspace/permissions/get
    """
    return api_request("GET", f"/api/2.0/permissions/genie/{space_id}")


def add_user_permission(space_id, user_name, permission_level):
    """
    Grant a permission level to a specific user (additive PATCH).

    PATCH /api/2.0/permissions/genie/{space_id}
    Docs: https://docs.databricks.com/api/workspace/permissions/update
    """
    payload = {
        "access_control_list": [
            {"user_name": user_name, "permission_level": permission_level}
        ]
    }
    return api_request("PATCH", f"/api/2.0/permissions/genie/{space_id}", json_data=payload)


def add_group_permission(space_id, group_name, permission_level):
    """
    Grant a permission level to a group (additive PATCH).

    PATCH /api/2.0/permissions/genie/{space_id}
    Docs: https://docs.databricks.com/api/workspace/permissions/update
    """
    payload = {
        "access_control_list": [
            {"group_name": group_name, "permission_level": permission_level}
        ]
    }
    return api_request("PATCH", f"/api/2.0/permissions/genie/{space_id}", json_data=payload)


def add_service_principal_permission(space_id, sp_name, permission_level):
    """
    Grant a permission level to a service principal (additive PATCH).

    PATCH /api/2.0/permissions/genie/{space_id}
    Docs: https://docs.databricks.com/api/workspace/permissions/update
    """
    payload = {
        "access_control_list": [
            {"service_principal_name": sp_name, "permission_level": permission_level}
        ]
    }
    return api_request("PATCH", f"/api/2.0/permissions/genie/{space_id}", json_data=payload)


def replace_all_permissions(space_id, access_control_list):
    """
    Replace ALL permissions on a Genie Space (destructive PUT).

    PUT /api/2.0/permissions/genie/{space_id}
    Docs: https://docs.databricks.com/api/workspace/permissions/set
    """
    payload = {"access_control_list": access_control_list}
    return api_request("PUT", f"/api/2.0/permissions/genie/{space_id}", json_data=payload)


def bulk_grant_example(space_id):
    """
    Example: grant multiple users and groups in a single PATCH.

    PATCH /api/2.0/permissions/genie/{space_id}
    Docs: https://docs.databricks.com/api/workspace/permissions/update
    """
    payload = {
        "access_control_list": [
            {"user_name": "analyst@company.com", "permission_level": "CAN_READ"},
            {"user_name": "data-engineer@company.com", "permission_level": "CAN_EDIT"},
            {"group_name": "finance-analysts", "permission_level": "CAN_RUN"},
            {"group_name": "finance-admins", "permission_level": "CAN_MANAGE"},
        ]
    }
    return api_request("PATCH", f"/api/2.0/permissions/genie/{space_id}", json_data=payload)


if __name__ == "__main__":
    from config import init_from_cli
    init_from_cli()

    print("=== Valid Permission Levels ===")
    levels = get_permission_levels(SPACE_ID)
    for level in levels.get("permission_levels", []):
        print(f"  {level['permission_level']}: {level['description']}")

    print("\n=== Current Permissions ===")
    perms = get_permissions(SPACE_ID)
    for acl in perms.get("access_control_list", []):
        principal = acl.get("user_name") or acl.get("group_name") or acl.get("service_principal_name", "unknown")
        levels = [p["permission_level"] for p in acl.get("all_permissions", [])]
        print(f"  {principal}: {', '.join(levels)}")
