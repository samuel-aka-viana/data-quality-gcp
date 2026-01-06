from datetime import datetime

import google.auth
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from google.cloud import dataplex_v1

# --- CONFIGURAÇÕES ---
PROJECT_ID = "data-quality-483423"
REGION = "us-central1"
REPOSITORY_ID = "workshop-data-quality"
WORKSPACE_ID = "workshop-dev"
# O ID do Scan que definimos no Terraform (google_dataplex_datascan)
DATAPLEX_SCAN_ID = "dq-scan-transactions"

default_args = {
    "owner": "workshop",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "catchup": False,
}


# --- FUNÇÃO PYTHON ATUALIZADA (DATAScAN) ---
def run_dataplex_manual(**kwargs):
    """
    Função atualizada para disparar o DataScan (AutoDQ)
    """
    # 1. Autenticação
    credentials, project = google.auth.default()

    # MUDANÇA 1: Usamos o cliente de DataScan agora
    client = dataplex_v1.DataScanServiceClient(credentials=credentials)

    # 2. Monta o caminho completo do recurso
    # MUDANÇA 2: O caminho agora é projects/.../locations/.../dataScans/...
    # (Não precisa mais passar pelo Lake na URL da API)
    name = f"projects/{PROJECT_ID}/locations/{REGION}/dataScans/{DATAPLEX_SCAN_ID}"

    print(f"Disparando DataScan: {name}")

    # 3. Executa o Scan
    # MUDANÇA 3: Request específico de DataScan
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
    # 1. Compila o código Dataform
    create_compilation = DataformCreateCompilationResultOperator(
        task_id="compile_project",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"
            ),
        },
    )

    # 2. Roda a transformação e assertions
    run_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="run_transformation_and_assertions",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_project')['name'] }}"
        },
    )

    # 3. Roda o scan de qualidade do Dataplex
    run_governance_scan = PythonOperator(
        task_id="run_dataplex_scan", python_callable=run_dataplex_manual
    )

    create_compilation >> run_workflow >> run_governance_scan
