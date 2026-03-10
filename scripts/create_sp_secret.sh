#!/usr/bin/env bash
# SP OAuth secret 생성 후 Databricks workspace secrets에 저장
# 환경변수: ACCOUNT_HOST, ACCOUNT_ID, SP_ID, WORKSPACE_HOST, AZURE_TENANT_ID
set -euo pipefail

TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --tenant "${AZURE_TENANT_ID}" --query accessToken -o tsv)

SECRET_JSON=$(curl -sf -X POST \
  "${ACCOUNT_HOST}/api/2.0/accounts/${ACCOUNT_ID}/servicePrincipals/${SP_ID}/credentials/secrets" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}')

SECRET_VALUE=$(echo "$SECRET_JSON" | jq -r '.secret')
if [ -z "$SECRET_VALUE" ] || [ "$SECRET_VALUE" = "null" ]; then
  echo "ERROR: SP secret 생성 실패" >&2
  echo "$SECRET_JSON" >&2
  exit 1
fi

DATABRICKS_HOST="https://${WORKSPACE_HOST}" databricks secrets put-secret admin sp-client-secret --string-value "$SECRET_VALUE"
echo "SP OAuth secret 생성 및 Databricks secrets 저장 완료"
