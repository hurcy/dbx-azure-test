#--------------------------------------------------------------
# Account-level Identity Bootstrap
# Azure Managed Identity -> Databricks Service Principal
# acc-sp: Account admin + Workspace ADMIN
#--------------------------------------------------------------

#--------------------------------------------------------------
# Azure Managed Identity for acc-sp
#--------------------------------------------------------------
resource "azurerm_user_assigned_identity" "acc_sp" {
  name                = "${local.resource_name}-acc-sp"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags
}

#--------------------------------------------------------------
# Account-level Service Principal (acc-sp)
#--------------------------------------------------------------
resource "databricks_service_principal" "acc_sp" {
  provider       = databricks.accounts
  application_id = azurerm_user_assigned_identity.acc_sp.client_id
  external_id    = azurerm_user_assigned_identity.acc_sp.principal_id
  display_name   = "${local.resource_name}-acc-sp"
}

#--------------------------------------------------------------
# acc-sp: Account admin Role
#--------------------------------------------------------------
resource "databricks_service_principal_role" "acc_sp_account_admin" {
  provider             = databricks.accounts
  service_principal_id = databricks_service_principal.acc_sp.id
  role                 = "account_admin"
}

#--------------------------------------------------------------
# Workspace Assignment - acc-sp (ADMIN)
#--------------------------------------------------------------
resource "databricks_mws_permission_assignment" "acc_sp" {
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.ws.workspace_id
  principal_id = databricks_service_principal.acc_sp.id
  permissions  = ["ADMIN"]
}

#--------------------------------------------------------------
# Federation Policy — GitHub Actions OIDC for acc-sp
#--------------------------------------------------------------
resource "databricks_service_principal_federation_policy" "acc_sp_github" {
  count                = var.enable_github_oidc_federation ? 1 : 0
  provider             = databricks.accounts
  service_principal_id = databricks_service_principal.acc_sp.id
  policy_id            = "github-actions"

  oidc_policy = {
    issuer    = "https://token.actions.githubusercontent.com"
    audiences = [var.databricks_account_id]
    subject   = "repo:${var.github_oidc_subject}"
  }
}
