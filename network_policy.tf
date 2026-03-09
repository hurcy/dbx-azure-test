#--------------------------------------------------------------
# Account-level Network Policy (Serverless Egress Control)
#--------------------------------------------------------------
resource "databricks_account_network_policy" "this" {
  provider          = databricks.accounts
  account_id        = var.databricks_account_id
  network_policy_id = "${local.resource_name}-policy"

  egress = {
    network_access = {
      restriction_mode = var.network_policy_restriction_mode

      allowed_storage_destinations = [{
        azure_storage_account    = azurerm_storage_account.storage.name
        azure_storage_service    = "dfs"
        storage_destination_type = "AZURE_STORAGE"
      }]

      allowed_internet_destinations = [
        for dest in var.allowed_internet_destinations : {
          destination              = dest.destination
          internet_destination_type = dest.type
        }
      ]

      policy_enforcement = {
        enforcement_mode = var.network_policy_enforcement_mode
      }
    }
  }
}

#--------------------------------------------------------------
# Bind Network Policy to Workspace
#--------------------------------------------------------------
resource "databricks_workspace_network_option" "this" {
  provider          = databricks.accounts
  workspace_id      = azurerm_databricks_workspace.ws.workspace_id
  network_policy_id = databricks_account_network_policy.this.network_policy_id
}
