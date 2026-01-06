from datetime import datetime

import google.auth
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from google.cloud import dataplex_v1

PROJECT_ID = "data-quality-483423"
REGION = "us-central1"
REPOSITORY_ID = "workshop-data-quality"
DATAPLEX_SCAN_ID = "dq-scan-transactions"
DATASET_ID = "workshop_gov"

default_args = {
    "owner": "workshop",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "catchup": False,
}

create_raw_sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}`;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.raw_transactions` AS
    -- 1. Dado Correto
    SELECT 'TXN-001' AS transaction_id, CURRENT_DATE() AS transaction_date, 101 AS store_id, 'PROD-A' AS product_sku, 50.00 AS amount, 'joao@email.com' AS customer_email UNION ALL
    -- 2. Dado Correto
    SELECT 'TXN-002', CURRENT_DATE(), 102, 'PROD-B', 120.50, 'maria@email.com' UNION ALL
    -- 3. ERRO: Valor Negativo (Dataform deve barrar na view curada)
    SELECT 'TXN-003', CURRENT_DATE(), 101, 'PROD-C', -10.00, 'erro_valor@email.com' UNION ALL
    -- 4. ERRO: ID Nulo (Dataform deve filtrar se tiver regra)
    SELECT NULL, CURRENT_DATE(), 103, 'PROD-D', 80.00, 'nulo@email.com' UNION ALL
    -- 5. ERRO: Data Futura (Dataplex ou Dataform podem validar)
    SELECT 'TXN-005', DATE_ADD(CURRENT_DATE(), INTERVAL 365 DAY), 101, 'PROD-A', 50.00, 'futuro@email.com' UNION ALL
    -- 6. AVISO: Email Inválido (Dataplex vai alertar conforme regra definida)
    SELECT 'TXN-006', CURRENT_DATE(), 102, 'PROD-B', 30.00, 'email_sem_arroba';
"""


def run_dataplex_manual(**kwargs):
    credentials, project = google.auth.default()
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)

    name = f"projects/{PROJECT_ID}/locations/{REGION}/dataScans/{DATAPLEX_SCAN_ID}"
    print(f"Disparando DataScan: {name}")

    request = dataplex_v1.RunDataScanRequest(name=name)
    response = client.run_data_scan(request=request)

    print(f"Scan disparado com sucesso! Job ID: {response.job.name}")
    return response.job.name


with DAG(
    "workshop_gov_pipeline",
    default_args=default_args,
    schedule_interval=None,
    tags=["dataform", "dataplex", "quality"],
    catchup=False,
) as dag:
    create_raw_data = BigQueryInsertJobOperator(
        task_id="create_raw_data",
        project_id=PROJECT_ID,
        location=REGION,
        configuration={
            "query": {
                "query": create_raw_sql,
                "useLegacySql": False,
            }
        },
    )

    # 2. Compila o código Dataform (lendo do Git 'main')
    create_compilation = DataformCreateCompilationResultOperator(
        task_id="compile_project",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": "main"},
    )

    run_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="run_transformation_and_assertions",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_project')['name'] }}"
        },
    )

    run_governance_scan = PythonOperator(
        task_id="run_dataplex_scan", python_callable=run_dataplex_manual
    )

    create_raw_data >> create_compilation >> run_workflow >> run_governance_scan
