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

#--------------------------------------------------------------
# Workspace IP Access List
#--------------------------------------------------------------
resource "databricks_workspace_conf" "ip_access" {
  count = var.enable_ip_access_list ? 1 : 0

  custom_config = {
    "enableIpAccessLists" = true
  }

  depends_on = [databricks_mws_permission_assignment.admin_user]
}

resource "databricks_ip_access_list" "allow" {
  count        = var.enable_ip_access_list ? 1 : 0
  label        = "${local.resource_name}-allow"
  list_type    = "ALLOW"
  ip_addresses = var.allowed_ip_addresses

  depends_on = [databricks_workspace_conf.ip_access]
}
