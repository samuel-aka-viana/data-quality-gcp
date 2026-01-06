variable "gcp_service_list" {
  description = "Lista de APIs necessárias para o workshop"
  type        = list(string)
  default = [
    "bigquery.googleapis.com",
    "dataform.googleapis.com",
    "dataplex.googleapis.com",
    "datacatalog.googleapis.com",
    "datalineage.googleapis.com",
    "workflows.googleapis.com",
    "secretmanager.googleapis.com"
  ]
}

resource "google_project_service" "gcp_services" {
  for_each = toset(var.gcp_service_list)
  project  = var.project_id
  service  = each.key
  disable_on_destroy = false
}
