data "databricks_metastore" "this" {
  provider = databricks.accounts
  name     = var.uc_metastore_name
  #region   = var.location
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.ws.workspace_id
  metastore_id = data.databricks_metastore.this.id
}

resource "databricks_grants" "metastore" {
  metastore = data.databricks_metastore.this.id
  grant {
    principal  = var.admin_user_email
    privileges = ["CREATE_EXTERNAL_LOCATION", "CREATE_STORAGE_CREDENTIAL"]
  }

  depends_on = [
    databricks_metastore_assignment.this,
    databricks_mws_permission_assignment.admin_user,
  ]
}

resource "databricks_default_namespace_setting" "this" {
  namespace {
    value = databricks_catalog.hurcy_ws_catalog.name
  }

  depends_on = [databricks_metastore_assignment.this]
}
