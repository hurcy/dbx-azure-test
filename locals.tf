locals {
  # Resource naming with optional random suffix
  name_suffix   = var.enable_random_suffix ? "-${random_string.suffix[0].result}" : ""
  resource_name = "${var.prefix}${local.name_suffix}"

  # Subnet names
  public_subnet_name  = "public-subnet"
  private_subnet_name = "private-subnet"
  pe_subnet_name      = "PE"
  pbi_subnet_name     = "PBI"

  # Subnet CIDR calculations (use variables if provided, otherwise calculate)
  public_subnet_cidr  = var.public_subnet_cidr != null ? var.public_subnet_cidr : cidrsubnet(var.cidr, 2, 0)
  private_subnet_cidr = var.private_subnet_cidr != null ? var.private_subnet_cidr : cidrsubnet(var.cidr, 2, 1)
  pe_subnet_cidr      = var.pe_subnet_cidr != null ? var.pe_subnet_cidr : cidrsubnet(var.cidr, 2, 2)
  pbi_subnet_cidr     = var.pbi_subnet_cidr != null ? var.pbi_subnet_cidr : cidrsubnet(var.cidr, 2, 3)

  # DBFS storage account resource ID
  dbfs_resource_id = "${azurerm_databricks_workspace.ws.managed_resource_group_id}/providers/Microsoft.Storage/storageAccounts/${azurerm_databricks_workspace.ws.custom_parameters[0].storage_account_name}"

  # Common tags
  tags = {
    Environment = var.environment
    Owner       = var.owner_email
    RemoveAfter = var.remove_after
    Description = var.description
  }

  # Databricks delegation actions
  databricks_delegation_actions = [
    "Microsoft.Network/virtualNetworks/subnets/join/action",
    "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
    "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
  ]
}
