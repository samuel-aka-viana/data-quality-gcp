import time
from datetime import datetime

import google.auth
from airflow import DAG
from airflow.exceptions import AirflowException
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
    SELECT 'TXN-001' AS transaction_id, CURRENT_DATE() AS transaction_date, 101 AS store_id, 'PROD-A' AS product_sku, 50.00 AS amount, 'joao@email.com' AS customer_email UNION ALL
    SELECT 'TXN-002', CURRENT_DATE(), 102, 'PROD-B', 120.50, 'maria@email.com' UNION ALL
    -- ERRO 1: Valor Negativo (Curated deve remover)
    SELECT 'TXN-003', CURRENT_DATE(), 101, 'PROD-C', -10.00, 'erro_valor@email.com' UNION ALL
    -- ERRO 2: Store ID Nulo (Regra do Dataplex deve pegar!)
    SELECT 'TXN-004', CURRENT_DATE(), NULL, 'PROD-D', 80.00, 'nulo@email.com' UNION ALL
    -- ERRO 3: Email Inválido (Regra do Dataplex deve pegar!)
    SELECT 'TXN-006', CURRENT_DATE(), 102, 'PROD-B', 30.00, 'email_sem_arroba';
"""


def run_dataplex_and_wait(**kwargs):
    """
    Dispara o DataScan, aguarda a execução e falha a task se a qualidade for ruim.
    """
    credentials, project = google.auth.default()
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)

    scan_name = f"projects/{PROJECT_ID}/locations/{REGION}/dataScans/{DATAPLEX_SCAN_ID}"
    print(f"🚀 Disparando DataScan: {scan_name}")

    request = dataplex_v1.RunDataScanRequest(name=scan_name)
    response = client.run_data_scan(request=request)
    job_name = response.job.name
    print(f"⏳ Scan iniciado. Job ID: {job_name}")

    while True:
        job = client.get_data_scan_job(name=job_name)
        state = job.state

        if state in [
            dataplex_v1.DataScanJob.State.RUNNING,
            dataplex_v1.DataScanJob.State.PENDING,
        ]:
            print(f"🔄 Scan em andamento... aguardando 10s.")
            time.sleep(10)
        elif state == dataplex_v1.DataScanJob.State.SUCCEEDED:
            print("✅ Execução do Scan finalizada com sucesso (técnico).")
            break
        elif state in [
            dataplex_v1.DataScanJob.State.FAILED,
            dataplex_v1.DataScanJob.State.CANCELLED,
        ]:
            raise AirflowException(
                f"❌ Erro na execução do Job (Crash/Cancelado): {job.message}"
            )
        else:
            print(f"Estado desconhecido: {state}")
            time.sleep(10)

    result = job.data_quality_result

    print(f"📊 Resultado Final: {'APROVADO' if result.passed else 'REPROVADO'}")
    print(f"🔍 Linhas verificadas: {result.row_count}")

    if not result.passed:
        error_msg = (
            "🚨 A QUALIDADE DOS DADOS FALHOU! Verifique o Dataplex para detalhes."
        )
        raise AirflowException(error_msg)

    return "Qualidade Aprovada!"


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
        task_id="run_dataplex_scan", python_callable=run_dataplex_and_wait
    )

    create_raw_data >> create_compilation >> run_workflow >> run_governance_scan
