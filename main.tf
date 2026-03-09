#--------------------------------------------------------------
# Random String for Unique Naming
#--------------------------------------------------------------
resource "random_string" "suffix" {
  count   = var.enable_random_suffix ? 1 : 0
  length  = 4
  lower   = true
  upper   = false
  special = false
}

#--------------------------------------------------------------
# Resource Group
#--------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = "${local.resource_name}-rg"
  location = var.location
  tags     = local.tags
}

#--------------------------------------------------------------
# Access Connector
#--------------------------------------------------------------
resource "azurerm_databricks_access_connector" "ac" {
  name                = "${local.resource_name}-ac"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

#--------------------------------------------------------------
# Azure Managed Identity for ws-sp
#--------------------------------------------------------------
resource "azurerm_user_assigned_identity" "ws_sp" {
  name                = "${local.resource_name}-ws-sp"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags
}

#--------------------------------------------------------------
# Storage Account
#--------------------------------------------------------------
resource "azurerm_storage_account" "storage" {
  name                     = replace("${var.prefix}${local.name_suffix}", "-", "")
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Enable hierarchical namespace for ADLS Gen2
  tags                     = local.tags
}

#--------------------------------------------------------------
# Storage Container
#--------------------------------------------------------------
resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

#--------------------------------------------------------------
# Role Assignment for Access Connector to Storage Account
#--------------------------------------------------------------
resource "azurerm_role_assignment" "ac_storage_contributor" {
  scope                = azurerm_storage_account.storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ac.identity[0].principal_id
}

#--------------------------------------------------------------
# Unity Catalog - Storage Credential
#--------------------------------------------------------------
resource "databricks_storage_credential" "storage_cred" {
  name = "${local.resource_name}-cred"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.ac.id
  }
  comment = "Storage credential for ${azurerm_storage_account.storage.name}"

  depends_on = [
    azurerm_role_assignment.ac_storage_contributor,
    databricks_metastore_assignment.this,
    databricks_grants.metastore,
  ]
}

resource "databricks_grants" "storage_cred_grants" {
  storage_credential = databricks_storage_credential.storage_cred.id
  grant {
    principal  = "account users"
    privileges = ["READ_FILES"]
  }
}

#--------------------------------------------------------------
# Unity Catalog - External Location
#--------------------------------------------------------------
resource "databricks_external_location" "data" {
  name            = "${local.resource_name}-data"
  url             = "abfss://data@${azurerm_storage_account.storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.storage_cred.name
  comment         = "External location for data container in ${azurerm_storage_account.storage.name}"

  depends_on = [
    databricks_storage_credential.storage_cred,
    databricks_grants.metastore,
  ]
}

resource "databricks_grants" "external_location_grants" {
  external_location = databricks_external_location.data.id
  grant {
    principal  = "account users"
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}

#--------------------------------------------------------------
# Unity Catalog - Catalog
#--------------------------------------------------------------
resource "databricks_catalog" "hurcy_ws_catalog" {
  name           = "hurcy_ws_catalog"
  storage_root   = databricks_external_location.data.url
  comment        = "Catalog for hurcy workspace data"
  isolation_mode = "OPEN"

  depends_on = [databricks_external_location.data]
}

resource "databricks_grants" "catalog_grants" {
  catalog = databricks_catalog.hurcy_ws_catalog.name
  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

#--------------------------------------------------------------
# Virtual Network
#--------------------------------------------------------------
resource "azurerm_virtual_network" "vn" {
  name                = "${local.resource_name}-vnet"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = [var.cidr]
  tags                = local.tags
}

#--------------------------------------------------------------
# Subnets
#--------------------------------------------------------------
resource "azurerm_subnet" "public" {
  name                 = local.public_subnet_name
  address_prefixes     = [local.public_subnet_cidr]
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vn.name

  delegation {
    name = "databricks_delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = local.databricks_delegation_actions
    }
  }
}

resource "azurerm_subnet" "private" {
  name                 = local.private_subnet_name
  address_prefixes     = [local.private_subnet_cidr]
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vn.name

  delegation {
    name = "databricks_delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = local.databricks_delegation_actions
    }
  }
}

resource "azurerm_subnet" "pe" {
  name                 = local.pe_subnet_name
  address_prefixes     = [local.pe_subnet_cidr]
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vn.name
}

resource "azurerm_subnet" "pbi" {
  name                 = local.pbi_subnet_name
  address_prefixes     = [local.pbi_subnet_cidr]
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vn.name

  delegation {
    name = "powerbi_delegation"
    service_delegation {
      name = "Microsoft.PowerPlatform/vnetaccesslinks"
    }
  }
}

#--------------------------------------------------------------
# Network Security Group - Compute (public + private subnets)
#--------------------------------------------------------------
resource "azurerm_network_security_group" "nsg" {
  name                = "${local.resource_name}-nsg"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  security_rule {
    name                       = "allow-vnet-outbound"
    priority                   = 100
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }

  security_rule {
    name                       = "allow-worker-to-sql"
    priority                   = 110
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "3306"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "Sql"
  }

  security_rule {
    name                       = "allow-worker-to-storage"
    priority                   = 120
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "Storage"
  }

  security_rule {
    name                       = "allow-worker-to-eventhub"
    priority                   = 130
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "9093"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "EventHub"
  }

  security_rule {
    name                       = "deny-all-outbound"
    priority                   = 4096
    direction                  = "Outbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "public_nsg" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

resource "azurerm_subnet_network_security_group_association" "private_nsg" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

#--------------------------------------------------------------
# Network Security Group - Private Endpoint subnet
#--------------------------------------------------------------
resource "azurerm_network_security_group" "pe_nsg" {
  name                = "${local.resource_name}-pe-nsg"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  security_rule {
    name                       = "allow-databricks-inbound"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["443", "3306", "6666", "8443-8451"]
    source_address_prefix      = "AzureDatabricks"
    destination_address_prefix = "VirtualNetwork"
  }

  security_rule {
    name                       = "allow-vnet-inbound"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["443", "3306", "6666", "8443-8451"]
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }
}

resource "azurerm_subnet_network_security_group_association" "pe_nsg" {
  subnet_id                 = azurerm_subnet.pe.id
  network_security_group_id = azurerm_network_security_group.pe_nsg.id
}

#--------------------------------------------------------------
# Private DNS Zones
#--------------------------------------------------------------
resource "azurerm_private_dns_zone" "blob" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_private_dns_zone" "dfs" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "dfs" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "${local.resource_name}-dfs-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.dfs[0].name
  virtual_network_id    = azurerm_virtual_network.vn.id
  tags                  = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "blob" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "${local.resource_name}-blob-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.blob[0].name
  virtual_network_id    = azurerm_virtual_network.vn.id
  tags                  = local.tags
}

#--------------------------------------------------------------
# Private Endpoints
#--------------------------------------------------------------
resource "azurerm_private_endpoint" "dfspe" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${local.resource_name}-dfs-pe"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.pe.id
  tags                = local.tags

  private_dns_zone_group {
    name                 = "add_to_azure_private_dns_dfs"
    private_dns_zone_ids = [azurerm_private_dns_zone.dfs[0].id]
  }

  private_service_connection {
    name                           = "${local.resource_name}-dfs"
    private_connection_resource_id = local.dbfs_resource_id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  depends_on = [azurerm_databricks_workspace.ws]
}

resource "azurerm_private_endpoint" "blobpe" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${local.resource_name}-blob-pe"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.pe.id
  tags                = local.tags

  private_dns_zone_group {
    name                 = "add_to_azure_private_dns_blob"
    private_dns_zone_ids = [azurerm_private_dns_zone.blob[0].id]
  }

  private_service_connection {
    name                           = "${local.resource_name}-blob"
    private_connection_resource_id = local.dbfs_resource_id
    subresource_names              = ["blob"]
    is_manual_connection           = false
  }

  depends_on = [azurerm_databricks_workspace.ws]
}

#--------------------------------------------------------------
# Databricks Workspace Private Endpoint (Backend Private Link)
#--------------------------------------------------------------
resource "azurerm_private_dns_zone" "databricks" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.azuredatabricks.net"
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "databricks" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "${local.resource_name}-databricks-link"
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.databricks[0].name
  virtual_network_id    = azurerm_virtual_network.vn.id
  tags                  = local.tags
}

resource "azurerm_private_endpoint" "databricks_backend" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${local.resource_name}-bak-pl-ep"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.pe.id
  tags                = local.tags

  custom_network_interface_name = "${local.resource_name}-bak-pl-ep-nic"

  private_dns_zone_group {
    name                 = "private-dns-zone-databricks"
    private_dns_zone_ids = [azurerm_private_dns_zone.databricks[0].id]
  }

  private_service_connection {
    name                           = "${local.resource_name}-databricks-ui-api"
    private_connection_resource_id = azurerm_databricks_workspace.ws.id
    subresource_names              = ["databricks_ui_api"]
    is_manual_connection           = false
  }

  depends_on = [azurerm_databricks_workspace.ws]
}

#--------------------------------------------------------------
# Databricks Workspace
#--------------------------------------------------------------
resource "azurerm_databricks_workspace" "ws" {
  name                                  = "${local.resource_name}-ws"
  resource_group_name                   = azurerm_resource_group.rg.name
  location                              = azurerm_resource_group.rg.location
  sku                                   = var.sku
  access_connector_id                   = azurerm_databricks_access_connector.ac.id
  default_storage_firewall_enabled      = var.default_storage_firewall_enabled
  network_security_group_rules_required = var.nsg_rules
  public_network_access_enabled         = var.public_network_access_enabled
  managed_resource_group_name           = "databricks-${local.resource_name}-mrg"
  tags                                  = local.tags

  custom_parameters {
    no_public_ip                                         = var.no_public_ip
    private_subnet_name                                  = azurerm_subnet.private.name
    public_subnet_name                                   = azurerm_subnet.public.name
    storage_account_sku_name                             = var.storage_account_sku
    virtual_network_id                                   = azurerm_virtual_network.vn.id
    public_subnet_network_security_group_association_id  = azurerm_network_security_group.nsg.id
    private_subnet_network_security_group_association_id = azurerm_network_security_group.nsg.id
  }

  depends_on = [
    azurerm_subnet_network_security_group_association.public_nsg,
    azurerm_subnet_network_security_group_association.private_nsg
  ]
}

#--------------------------------------------------------------
# Databricks Workspace Admin
#--------------------------------------------------------------
data "databricks_user" "admin_user" {
  provider   = databricks.accounts
  user_name  = var.admin_user_email
}

resource "databricks_mws_permission_assignment" "admin_user" {
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.ws.workspace_id
  principal_id = data.databricks_user.admin_user.id
  permissions  = ["ADMIN"]
}

#--------------------------------------------------------------
# Workspace-level Service Principal (ws-sp, no Roles)
#--------------------------------------------------------------
resource "databricks_service_principal" "ws_sp" {
  provider       = databricks.accounts
  application_id = azurerm_user_assigned_identity.ws_sp.client_id
  external_id    = azurerm_user_assigned_identity.ws_sp.principal_id
  display_name   = "${local.resource_name}-ws-sp"
}

resource "databricks_mws_permission_assignment" "ws_sp" {
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.ws.workspace_id
  principal_id = databricks_service_principal.ws_sp.id
  permissions  = ["ADMIN"]
}
