#--------------------------------------------------------------
# Resource Group Outputs
#--------------------------------------------------------------
output "resource_group_name" {
  description = "Name of the resource group"
  value       = azurerm_resource_group.rg.name
}

output "resource_group_id" {
  description = "ID of the resource group"
  value       = azurerm_resource_group.rg.id
}

#--------------------------------------------------------------
# Network Outputs
#--------------------------------------------------------------
output "virtual_network_id" {
  description = "ID of the virtual network"
  value       = azurerm_virtual_network.vn.id
}

output "virtual_network_name" {
  description = "Name of the virtual network"
  value       = azurerm_virtual_network.vn.name
}

output "public_subnet_id" {
  description = "ID of the public subnet"
  value       = azurerm_subnet.public.id
}

output "private_subnet_id" {
  description = "ID of the private subnet"
  value       = azurerm_subnet.private.id
}

output "pe_subnet_id" {
  description = "ID of the private endpoint subnet"
  value       = azurerm_subnet.pe.id
}

#--------------------------------------------------------------
# Databricks Workspace Outputs
#--------------------------------------------------------------
output "databricks_workspace_id" {
  description = "ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.ws.id
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.ws.workspace_url
}

output "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  value       = azurerm_databricks_workspace.ws.name
}

output "databricks_managed_resource_group_id" {
  description = "ID of the Databricks managed resource group"
  value       = azurerm_databricks_workspace.ws.managed_resource_group_id
}

#--------------------------------------------------------------
# Access Connector Outputs
#--------------------------------------------------------------
output "access_connector_id" {
  description = "ID of the Databricks access connector"
  value       = azurerm_databricks_access_connector.ac.id
}

output "access_connector_principal_id" {
  description = "Principal ID of the access connector managed identity"
  value       = azurerm_databricks_access_connector.ac.identity[0].principal_id
}

#--------------------------------------------------------------
# ws-sp Managed Identity Outputs
#--------------------------------------------------------------
output "ws_sp_managed_identity_id" {
  description = "ID of the ws-sp managed identity"
  value       = azurerm_user_assigned_identity.ws_sp.id
}

output "ws_sp_managed_identity_principal_id" {
  description = "Principal ID of the ws-sp managed identity"
  value       = azurerm_user_assigned_identity.ws_sp.principal_id
}

output "ws_sp_managed_identity_client_id" {
  description = "Client ID of the ws-sp managed identity"
  value       = azurerm_user_assigned_identity.ws_sp.client_id
}

#--------------------------------------------------------------
# Storage Account Outputs
#--------------------------------------------------------------
output "storage_account_id" {
  description = "ID of the storage account"
  value       = azurerm_storage_account.storage.id
}

output "storage_account_name" {
  description = "Name of the storage account"
  value       = azurerm_storage_account.storage.name
}

output "storage_account_primary_dfs_endpoint" {
  description = "Primary DFS endpoint of the storage account"
  value       = azurerm_storage_account.storage.primary_dfs_endpoint
}

output "storage_container_data_name" {
  description = "Name of the data container"
  value       = azurerm_storage_container.data.name
}

#--------------------------------------------------------------
# Unity Catalog Outputs
#--------------------------------------------------------------
output "storage_credential_name" {
  description = "Name of the storage credential"
  value       = databricks_storage_credential.storage_cred.name
}

output "external_location_name" {
  description = "Name of the external location"
  value       = databricks_external_location.data.name
}

output "external_location_url" {
  description = "URL of the external location"
  value       = databricks_external_location.data.url
}

output "catalog_name" {
  description = "Name of the Unity Catalog"
  value       = databricks_catalog.hurcy_ws_catalog.name
}

#--------------------------------------------------------------
# Private Endpoint Outputs
#--------------------------------------------------------------
output "dfs_private_endpoint_id" {
  description = "ID of the DFS private endpoint"
  value       = var.enable_private_endpoints ? azurerm_private_endpoint.dfspe[0].id : null
}

output "blob_private_endpoint_id" {
  description = "ID of the Blob private endpoint"
  value       = var.enable_private_endpoints ? azurerm_private_endpoint.blobpe[0].id : null
}

output "databricks_backend_private_endpoint_id" {
  description = "ID of the Databricks workspace backend private endpoint"
  value       = var.enable_private_endpoints ? azurerm_private_endpoint.databricks_backend[0].id : null
}

output "databricks_private_dns_zone_id" {
  description = "ID of the Databricks private DNS zone"
  value       = var.enable_private_endpoints ? azurerm_private_dns_zone.databricks[0].id : null
}

#--------------------------------------------------------------
# ws-sp Service Principal Outputs
#--------------------------------------------------------------
output "ws_sp_id" {
  description = "ID of the ws-sp Databricks service principal"
  value       = databricks_service_principal.ws_sp.id
}

output "ws_sp_application_id" {
  description = "Application ID of the ws-sp Databricks service principal"
  value       = databricks_service_principal.ws_sp.application_id
}

#--------------------------------------------------------------
# acc-sp Identity Outputs
#--------------------------------------------------------------
output "acc_sp_managed_identity_id" {
  description = "ID of the acc-sp managed identity"
  value       = azurerm_user_assigned_identity.acc_sp.id
}

output "acc_sp_managed_identity_client_id" {
  description = "Client ID of the acc-sp managed identity"
  value       = azurerm_user_assigned_identity.acc_sp.client_id
}

output "acc_sp_id" {
  description = "ID of the acc-sp Databricks service principal"
  value       = databricks_service_principal.acc_sp.id
}

output "acc_sp_application_id" {
  description = "Application ID of the acc-sp Databricks service principal"
  value       = databricks_service_principal.acc_sp.application_id
}

#--------------------------------------------------------------
# Federation Policy Outputs
#--------------------------------------------------------------
output "acc_sp_federation_policy_id" {
  description = "ID of the acc-sp GitHub Actions federation policy"
  value       = var.enable_github_oidc_federation ? databricks_service_principal_federation_policy.acc_sp_github[0].uid : null
}
