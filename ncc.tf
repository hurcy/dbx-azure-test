#--------------------------------------------------------------
# Network Connectivity Config (NCC) for Serverless Access
#--------------------------------------------------------------
resource "databricks_mws_network_connectivity_config" "this" {
  provider = databricks.accounts
  name     = "${local.resource_name}-ncc"
  region   = var.location
}

#--------------------------------------------------------------
# NCC Private Endpoint Rules - DBFS Managed Storage
#--------------------------------------------------------------
resource "databricks_mws_ncc_private_endpoint_rule" "dbfs_dfs" {
  provider                       = databricks.accounts
  network_connectivity_config_id = databricks_mws_network_connectivity_config.this.network_connectivity_config_id
  resource_id                    = local.dbfs_resource_id
  group_id                       = "dfs"
}

resource "databricks_mws_ncc_private_endpoint_rule" "dbfs_blob" {
  provider                       = databricks.accounts
  network_connectivity_config_id = databricks_mws_network_connectivity_config.this.network_connectivity_config_id
  resource_id                    = local.dbfs_resource_id
  group_id                       = "blob"
}

#--------------------------------------------------------------
# NCC Private Endpoint Rules - External Storage
#--------------------------------------------------------------
resource "databricks_mws_ncc_private_endpoint_rule" "storage_dfs" {
  provider                       = databricks.accounts
  network_connectivity_config_id = databricks_mws_network_connectivity_config.this.network_connectivity_config_id
  resource_id                    = azurerm_storage_account.storage.id
  group_id                       = "dfs"
}

resource "databricks_mws_ncc_private_endpoint_rule" "storage_blob" {
  provider                       = databricks.accounts
  network_connectivity_config_id = databricks_mws_network_connectivity_config.this.network_connectivity_config_id
  resource_id                    = azurerm_storage_account.storage.id
  group_id                       = "blob"
}

#--------------------------------------------------------------
# Bind NCC to Workspace
#--------------------------------------------------------------
resource "databricks_mws_ncc_binding" "this" {
  provider                       = databricks.accounts
  network_connectivity_config_id = databricks_mws_network_connectivity_config.this.network_connectivity_config_id
  workspace_id                   = azurerm_databricks_workspace.ws.workspace_id

  depends_on = [
    databricks_mws_ncc_private_endpoint_rule.dbfs_dfs,
    databricks_mws_ncc_private_endpoint_rule.dbfs_blob,
    databricks_mws_ncc_private_endpoint_rule.storage_dfs,
    databricks_mws_ncc_private_endpoint_rule.storage_blob,
  ]
}
