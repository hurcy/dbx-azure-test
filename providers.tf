terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">=4.0.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.52.0"
    }
    random = {
      source = "hashicorp/random"
    }
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
}

provider "random" {}

# Databricks provider — auth_type is determined by environment variables.
# Phase 1 (Bootstrap): `az login` → azure-cli auto-detected
# Phase 2 (Steady-state): set DATABRICKS_AUTH_TYPE + DATABRICKS_CLIENT_ID → OIDC/MSI
provider "databricks" {
  host = azurerm_databricks_workspace.ws.workspace_url
}

provider "databricks" {
  alias           = "accounts"
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.databricks_account_id
  azure_tenant_id = var.azure_tenant_id
}
