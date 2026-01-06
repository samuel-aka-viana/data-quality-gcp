variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
  default     = "data-quality-483423"
}

variable "user_email" {
  description = "Email do usuário que fará o workshop (para permissões)"
  type        = string
  default     = "samuelviana2626@gmail.com"
}

variable "region" {
  description = "Região dos recursos (ex: us-central1)"
  type        = string
  default     = "us-central1"
}

variable "git_variable" {
  description = "com permissão de repo"
  type        = string
  sensitive   = true
}
