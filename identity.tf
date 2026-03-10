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

#--------------------------------------------------------------
# acc-sp OAuth Secret → Databricks Secret Scope
# 클러스터에서 Account API 호출 시 사용
#--------------------------------------------------------------
resource "databricks_secret_scope" "admin" {
  name = "admin"

  depends_on = [databricks_mws_permission_assignment.acc_sp]
}

resource "databricks_secret" "account_id" {
  scope        = databricks_secret_scope.admin.name
  key          = "databricks-account-id"
  string_value = var.databricks_account_id
}

resource "databricks_secret" "sp_client_id" {
  scope        = databricks_secret_scope.admin.name
  key          = "sp-client-id"
  string_value = databricks_service_principal.acc_sp.application_id
}

# SP OAuth secret 생성 → Databricks workspace secret에 저장
resource "terraform_data" "acc_sp_secret" {
  depends_on = [databricks_secret_scope.admin]
  triggers_replace = [databricks_service_principal.acc_sp.id]

  provisioner "local-exec" {
    command = "bash ${path.module}/scripts/create_sp_secret.sh"
    environment = {
      ACCOUNT_HOST   = "https://accounts.azuredatabricks.net"
      ACCOUNT_ID     = var.databricks_account_id
      SP_ID          = databricks_service_principal.acc_sp.id
      WORKSPACE_HOST = azurerm_databricks_workspace.ws.workspace_url
      AZURE_TENANT_ID = var.azure_tenant_id
    }
  }
}
