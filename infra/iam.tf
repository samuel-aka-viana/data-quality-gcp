resource "google_project_iam_member" "user_roles" {
  for_each = toset([
    "roles/bigquery.user",
    "roles/bigquery.dataEditor",
    "roles/dataform.editor",
    "roles/dataplex.editor",
    "roles/dataplex.admin",
    "roles/serviceusage.serviceUsageConsumer",
    "roles/storage.objectAdmin"
  ])

  role    = each.key
  member  = "user:${var.user_email}"
  project = var.project_id
}

resource "google_service_account" "dataplex_sa" {
  account_id   = "dataplex-dq-worker"
  display_name = "Service Account para Dataplex Data Quality"
  project      = var.project_id
}

resource "google_project_iam_member" "dataplex_permissions" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/dataplex.metadataWriter",
    "roles/logging.logWriter"
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataplex_sa.email}"
}
