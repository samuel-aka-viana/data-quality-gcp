data "google_project" "project" {
  project_id = var.project_id
}

resource "google_secret_manager_secret" "dataform_git_secret" {
  secret_id = "dataform-github-token"

  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.gcp_services]
}

resource "google_secret_manager_secret_version" "dataform_git_secret_version" {
  secret = google_secret_manager_secret.dataform_git_secret.id
  secret_data = var.git_variable
}

resource "google_secret_manager_secret_iam_member" "dataform_secret_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.dataform_git_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-dataform.iam.gserviceaccount.com"

  depends_on = [google_secret_manager_secret.dataform_git_secret]
}

resource "google_bigquery_dataset" "workshop_ds" {
  dataset_id                  = "workshop_gov"
  friendly_name               = "Workshop Governance"
  description                 = "Dataset contendo dados brutos e curados do workshop"
  location                    = var.region
  project                     = var.project_id
  delete_contents_on_destroy  = true

  depends_on = [google_project_service.gcp_services]
}

resource "google_dataform_repository" "workshop_repo" {
  provider = google-beta
  name     = "workshop-data-quality"
  region   = var.region
  project  = var.project_id

  git_remote_settings {
      url = "https://github.com/samuel-aka-viana/data-quality-gcp.git"
      default_branch = "main"

      authentication_token_secret_version = google_secret_manager_secret_version.dataform_git_secret_version.id
  }

  depends_on = [
    google_project_service.gcp_services,
    google_secret_manager_secret_iam_member.dataform_secret_access
  ]
}

# --- 4. Dataplex Lake ---
resource "google_dataplex_lake" "workshop_lake" {
  name         = "retail-lake"
  location     = var.region
  project      = var.project_id
  display_name = "Retail Data Lake"

  depends_on = [google_project_service.gcp_services]
}

# --- 5. Dataplex Zone (Curated) ---
resource "google_dataplex_zone" "curated_zone" {
  name         = "curated-zone"
  location     = var.region
  lake         = google_dataplex_lake.workshop_lake.name
  type         = "CURATED"

  discovery_spec {
    enabled = true
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  project = var.project_id
}

# --- 6. Dataplex Asset (Conexão com BigQuery) ---
resource "google_dataplex_asset" "bq_asset" {
  name          = "workshop-transactions"
  location      = var.region
  lake          = google_dataplex_lake.workshop_lake.name
  dataplex_zone = google_dataplex_zone.curated_zone.name

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${google_bigquery_dataset.workshop_ds.dataset_id}"
    type = "BIGQUERY_DATASET"
  }

  project = var.project_id

  depends_on = [google_bigquery_dataset.workshop_ds]
}

# --- 7. Dataplex Data Scan (Regras de Qualidade) ---
# Substituímos a "Task" antiga pelo "DataScan" moderno
resource "google_dataplex_datascan" "dq_scan" {
  data_scan_id = "dq-scan-transactions"
  project      = var.project_id
  location     = var.region

  data {
    # Caminho completo da tabela que será verificada
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/${google_bigquery_dataset.workshop_ds.dataset_id}/tables/transactions_curated"
  }

  execution_spec {
    trigger {
      on_demand {} # Será acionado pelo Airflow
    }
  }

  data_quality_spec {
    # Regra 1: Validação de E-mail
    rules {
      name        = "validar_email"
      description = "O email deve conter @"
      dimension   = "CONFORMITY"
      threshold   = 0.95
      column      = "customer_email"
      regex_expectation {
        regex = "^[^@]+@[^@]+\\.[^@]+$"
      }
    }

    # Regra 2: Store ID obrigatório
    rules {
      name        = "store_id_nao_nulo"
      description = "Store ID é obrigatório"
      dimension   = "COMPLETENESS"
      threshold   = 1.0
      column      = "store_id"
      non_null_expectation {}
    }
  }

  depends_on = [
    google_dataplex_asset.bq_asset
  ]
}
