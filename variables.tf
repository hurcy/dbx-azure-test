#--------------------------------------------------------------
# Azure Subscription
#--------------------------------------------------------------
variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

#--------------------------------------------------------------
# Common Configuration
#--------------------------------------------------------------
variable "location" {
  description = "Azure region where resources will be created"
  type        = string
  default     = "northeurope"
}

variable "prefix" {
  description = "Prefix for resource naming"
  type        = string
}

variable "enable_random_suffix" {
  description = "Enable random suffix for unique resource naming"
  type        = bool
  default     = true
}

#--------------------------------------------------------------
# Tags
#--------------------------------------------------------------
variable "environment" {
  description = "Environment name for tagging"
  type        = string
  default     = "Demo"
}

variable "owner_email" {
  description = "Owner email for tagging and Databricks admin"
  type        = string
}

variable "remove_after" {
  description = "Resource removal date for tagging (YYYY-MM-DD)"
  type        = string
}

variable "description" {
  description = "Description for tagging"
  type        = string
  default     = ""
}

#--------------------------------------------------------------
# Network Configuration
#--------------------------------------------------------------
variable "cidr" {
  description = "CIDR range for the virtual network"
  type        = string
  default     = "100.64.0.0/23"
}

variable "public_subnet_cidr" {
  description = "CIDR range for public subnet (optional, will use cidrsubnet if not specified)"
  type        = string
  default     = null
}

variable "private_subnet_cidr" {
  description = "CIDR range for private subnet (optional, will use cidrsubnet if not specified)"
  type        = string
  default     = null
}

variable "pe_subnet_cidr" {
  description = "CIDR range for private endpoint subnet (optional, will use cidrsubnet if not specified)"
  type        = string
  default     = null
}

variable "pbi_subnet_cidr" {
  description = "CIDR range for Power BI subnet (optional, will use cidrsubnet if not specified)"
  type        = string
  default     = null
}

#--------------------------------------------------------------
# Databricks Account Configuration
#--------------------------------------------------------------
variable "databricks_account_id" {
  description = "Databricks Account Console ID for account-level operations"
  type        = string
}

variable "azure_tenant_id" {
  description = "Azure AD tenant ID where the Databricks account resides"
  type        = string
}

variable "uc_metastore_name" {
  description = "Name of the existing Unity Catalog metastore to assign to the workspace"
  type        = string
}

#--------------------------------------------------------------
# Databricks Workspace Configuration
#--------------------------------------------------------------
variable "sku" {
  description = "Databricks workspace SKU (standard, premium, or trial)"
  type        = string
  default     = "premium"
}

variable "nsg_rules" {
  description = "NSG rules for Databricks (AllRules, NoAzureDatabricksRules, NoAzureServiceRules)"
  type        = string
  default     = "AllRules"
}

variable "public_network_access_enabled" {
  description = "Enable public network access to workspace"
  type        = bool
  default     = true
}

variable "default_storage_firewall_enabled" {
  description = "Enable storage firewall for DBFS"
  type        = bool
  default     = true
}

variable "no_public_ip" {
  description = "Disable public IP for compute nodes (Secure Cluster Connectivity)"
  type        = bool
  default     = true
}

variable "storage_account_sku" {
  description = "Storage account SKU for DBFS"
  type        = string
  default     = "Standard_GRS"
}

#--------------------------------------------------------------
# Private Endpoint Configuration
#--------------------------------------------------------------
variable "enable_private_endpoints" {
  description = "Enable private endpoints for DBFS storage"
  type        = bool
  default     = true
}

#--------------------------------------------------------------
# Network Policy Configuration
#--------------------------------------------------------------
variable "network_policy_restriction_mode" {
  description = "Serverless egress restriction mode (FULL_ACCESS or RESTRICTED_ACCESS)"
  type        = string
  default     = "FULL_ACCESS"
}

variable "network_policy_enforcement_mode" {
  description = "Network policy enforcement mode (ENFORCED or DRY_RUN)"
  type        = string
  default     = "DRY_RUN"
}

variable "allowed_internet_destinations" {
  description = "List of allowed internet destinations for serverless egress"
  type = list(object({
    destination = string
    type        = string
  }))
  default = []
}

variable "enable_ip_access_list" {
  description = "Enable IP-based access control for the workspace"
  type        = bool
  default     = false
}

variable "allowed_ip_addresses" {
  description = "List of allowed IP addresses/CIDRs for workspace access"
  type        = list(string)
  default     = []
}

#--------------------------------------------------------------
# Databricks Admin User Configuration
#--------------------------------------------------------------
variable "admin_user_email" {
  description = "Databricks Workspace 관리자로 지정할 사용자의 Entra ID 이메일 주소"
  type        = string
  default     = null
}

#--------------------------------------------------------------
# Workload Identity Federation
#--------------------------------------------------------------
variable "enable_github_oidc_federation" {
  description = "Enable GitHub Actions OIDC federation for acc-sp"
  type        = bool
  default     = false
}

variable "github_oidc_subject" {
  description = "GitHub OIDC subject (e.g. my-org/my-repo:environment:prod)"
  type        = string
  default     = ""
}
