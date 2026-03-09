"""
사전준비사항
- Databricks Service Principal에 Account admin 권한 부여
- Job에서 해당 Service Principal로 "Run As" 설정
- 환경변수: DATABRICKS_ACCOUNT_ID, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET

Databricks Network Policy 정보 조회 및 이력 저장 스크립트 (REST API 버전)

이 스크립트는 requests 모듈을 사용하여 REST API로:
1. 현재 워크스페이스에 설정된 network_policy_id를 획득
2. 해당 네트워크 정책의 상세 정보를 조회
3. SCD Type 1 패턴으로 Delta 테이블에 이력 저장 (변경 시에만)

API 참고:
- https://docs.databricks.com/api/azure/account/workspacenetworkconfiguration/getworkspacenetworkoptionrpc
- https://docs.databricks.com/api/azure/account/networkpolicies/getnetworkpolicyrpc
"""

import json
import hashlib
import os
from datetime import datetime

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType

# =============================================================================
# Databricks Account 인증 정보 (상수)
# =============================================================================
DATABRICKS_HOST = "https://accounts.azuredatabricks.net"
DATABRICKS_ACCOUNT_ID = os.environ.get("DATABRICKS_ACCOUNT_ID", "")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

# =============================================================================
# 조회할 워크스페이스 ID
# =============================================================================
WORKSPACE_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)

# =============================================================================
# 이력 저장 테이블 (catalog.schema.table)
# =============================================================================
TARGET_TABLE = "hurcy_catalog.default.network_policy_history"

# Debug 모드: Job parameter(spark.conf job_parameter.debug) 또는 환경변수 DEBUG
DEBUG_MODE = False


def _resolve_debug_mode(spark) -> bool:
    """Spark conf 또는 환경변수에서 debug 여부를 읽어 DEBUG_MODE를 설정하고 반환합니다."""
    global DEBUG_MODE
    try:
        val = spark.conf.get("job_parameter.debug", os.environ.get("DEBUG", "false"))
    except Exception:
        val = os.environ.get("DEBUG", "false")
    DEBUG_MODE = str(val).lower() in ("true", "1", "yes")
    return DEBUG_MODE


def log_info(msg: str) -> None:
    """요약/결과 등 정보 메시지 (항상 출력)."""
    print(msg)


def log_debug(msg: str) -> None:
    """상세 메시지 (debug 모드일 때만 출력)."""
    if DEBUG_MODE:
        print(msg)


def get_access_token() -> str:
    """OAuth2 Client Credentials를 사용하여 액세스 토큰을 획득합니다."""
    token_url = f"{DATABRICKS_HOST}/oidc/accounts/{DATABRICKS_ACCOUNT_ID}/v1/token"
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET),
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_workspace_network_option(access_token: str, workspace_id: int) -> dict:
    """
    워크스페이스의 네트워크 옵션을 조회합니다.

    API: GET /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/network
    """
    url = f"{DATABRICKS_HOST}/api/2.0/accounts/{DATABRICKS_ACCOUNT_ID}/workspaces/{workspace_id}/network"

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    )

    response.raise_for_status()
    return response.json()


def get_network_policy(access_token: str, network_policy_id: str) -> dict:
    """
    네트워크 정책 상세 정보를 조회합니다.

    API: GET /api/2.0/accounts/{account_id}/network-policies/{network_policy_id}
    """
    url = f"{DATABRICKS_HOST}/api/2.0/accounts/{DATABRICKS_ACCOUNT_ID}/network-policies/{network_policy_id}"

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    )

    response.raise_for_status()
    return response.json()


def get_network_policy_details(workspace_id: int) -> dict:
    """
    워크스페이스의 네트워크 정책 상세 정보를 조회합니다.
    """
    log_debug("[0/2] 액세스 토큰 획득 중...")
    access_token = get_access_token()
    log_debug("      - 토큰 획득 완료")

    log_debug(f"[1/2] 워크스페이스 {workspace_id}의 네트워크 옵션 조회 중...")
    workspace_network_option = get_workspace_network_option(access_token, workspace_id)

    network_policy_id = workspace_network_option.get("network_policy_id")
    log_debug(f"      - Network Policy ID: {network_policy_id}")

    log_debug(f"[2/2] 네트워크 정책 '{network_policy_id}' 상세 정보 조회 중...")
    network_policy = get_network_policy(access_token, network_policy_id)

    return {
        "workspace_id": workspace_id,
        "workspace_network_option": workspace_network_option,
        "network_policy": network_policy,
        "network_policy_id": network_policy_id
    }


def compute_hash(data: dict) -> str:
    """딕셔너리 데이터의 SHA256 해시값을 계산합니다."""
    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """테이블 존재 여부를 확인합니다."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def get_latest_hash(spark: SparkSession, table_name: str, workspace_id: int) -> str:
    """테이블에서 해당 워크스페이스의 최신 해시값을 조회합니다."""
    try:
        df = spark.sql(f"""
            SELECT policy_hash
            FROM {table_name}
            WHERE workspace_id = {workspace_id}
            ORDER BY captured_at DESC
            LIMIT 1
        """)

        if df.count() > 0:
            return df.first()["policy_hash"]
        return None
    except Exception:
        return None


def create_history_table(spark: SparkSession, table_name: str):
    """이력 테이블을 생성합니다."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            workspace_id LONG COMMENT '워크스페이스 ID',
            network_policy_id STRING COMMENT '네트워크 정책 ID',
            policy_data STRING COMMENT '전체 정책 데이터 (JSON)',
            policy_hash STRING COMMENT '정책 데이터 해시값 (SHA256)',
            captured_at TIMESTAMP COMMENT '캡처 시간',
            is_current BOOLEAN COMMENT '현재 유효한 레코드 여부'
        )
        USING DELTA
        COMMENT 'Network Policy 변경 이력 테이블 (SCD Type 1)'
    """)
    log_debug(f"테이블 생성 완료: {table_name}")


def save_network_policy_history(
    spark: SparkSession,
    result: dict,
    table_name: str
) -> bool:
    """
    네트워크 정책 정보를 SCD1 패턴으로 Delta 테이블에 저장합니다.
    변경이 있는 경우에만 새 행을 추가합니다.
    """
    workspace_id = result["workspace_id"]
    network_policy = result["network_policy"]

    current_hash = compute_hash(result)

    if not table_exists(spark, table_name):
        log_debug(f"테이블이 존재하지 않습니다. 새로 생성합니다: {table_name}")
        create_history_table(spark, table_name)

    latest_hash = get_latest_hash(spark, table_name, workspace_id)

    if latest_hash is not None and latest_hash == current_hash:
        log_info(f"[변경 없음] 워크스페이스 {workspace_id}의 네트워크 정책에 변경이 없습니다.")
        return False

    if latest_hash is None:
        log_info(f"[최초 입력] 워크스페이스 {workspace_id}의 네트워크 정책 이력이 없어 첫 행을 입력합니다.")
    else:
        log_info(f"[변경 감지] 워크스페이스 {workspace_id}의 네트워크 정책이 변경되었습니다.")

    if latest_hash is not None:
        spark.sql(f"""
            UPDATE {table_name}
            SET is_current = FALSE
            WHERE workspace_id = {workspace_id} AND is_current = TRUE
        """)

    current_time = datetime.now()

    new_row = [(
        workspace_id,
        network_policy.get("network_policy_id"),
        json.dumps(result, ensure_ascii=False),
        current_hash,
        current_time,
        True
    )]

    schema = StructType([
        StructField("workspace_id", LongType(), False),
        StructField("network_policy_id", StringType(), True),
        StructField("policy_data", StringType(), True),
        StructField("policy_hash", StringType(), False),
        StructField("captured_at", TimestampType(), False),
        StructField("is_current", BooleanType(), False)
    ])

    new_df = spark.createDataFrame(new_row, schema)
    new_df.write.mode("append").saveAsTable(table_name)

    log_info("[저장 완료] 새 레코드가 추가되었습니다.")
    log_debug(f"  - Policy ID: {network_policy.get('network_policy_id')}")
    log_debug(f"  - Hash: {current_hash[:16]}...")
    log_debug(f"  - Captured At: {current_time}")

    return True


def display_network_policy(result: dict):
    """네트워크 정책 정보를 보기 좋게 출력합니다. (debug 모드에서만 상세 출력)"""
    log_debug("\n" + "=" * 60)
    log_debug("네트워크 정책 상세 정보")
    log_debug("=" * 60)
    log_debug(f"Workspace ID: {result['workspace_id']}")

    network_policy = result.get('network_policy', {})
    log_debug(f"Network Policy ID: {network_policy.get('network_policy_id')}")

    egress = network_policy.get('egress')
    if egress:
        log_debug("\n[Egress 정책]")
        network_access = egress.get('network_access', {})

        log_debug(f"  Restriction Mode: {network_access.get('restriction_mode')}")

        policy_enforcement = network_access.get('policy_enforcement', {})
        if policy_enforcement:
            log_debug(f"  Enforcement Mode: {policy_enforcement.get('enforcement_mode')}")
            dry_run_products = policy_enforcement.get('dry_run_mode_product_filter', [])
            if dry_run_products:
                log_debug(f"  Dry Run Products: {', '.join(dry_run_products)}")

        allowed_internet = network_access.get('allowed_internet_destinations', [])
        if allowed_internet:
            log_debug("\n  [Allowed Internet Destinations]")
            for dest in allowed_internet:
                log_debug(f"    - {dest.get('destination')} ({dest.get('internet_destination_type')})")

        allowed_storage = network_access.get('allowed_storage_destinations', [])
        if allowed_storage:
            log_debug("\n  [Allowed Storage Destinations]")
            for dest in allowed_storage:
                log_debug(f"    - Account: {dest.get('azure_storage_account')}")
                log_debug(f"      Service: {dest.get('azure_storage_service')}")
                log_debug(f"      Paths: {dest.get('allowed_paths')}")

    log_debug("=" * 60)


# =============================================================================
# 실행
# =============================================================================

_resolve_debug_mode(spark)
if DEBUG_MODE:
    log_info("(debug 모드: 상세 로그 출력)")

if not DATABRICKS_CLIENT_ID or not DATABRICKS_CLIENT_SECRET:
    raise ValueError(
        "DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET 환경변수가 필요합니다. "
        "Account Admin 권한이 있는 Service Principal의 credentials를 설정하세요."
    )

result = get_network_policy_details(WORKSPACE_ID)
log_info(f"워크스페이스 {result['workspace_id']} 네트워크 정책 조회 완료.")
display_network_policy(result)

is_changed = save_network_policy_history(spark, result, TARGET_TABLE)
if is_changed:
    log_info(f"\n[결과] 새로운 정책 변경 이력이 저장되었습니다: {TARGET_TABLE}")
else:
    log_info(f"\n[결과] 정책 변경이 없어 이력이 추가되지 않았습니다.")
