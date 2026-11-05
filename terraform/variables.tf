###############################################################################
# S24 – Online Video Learning :: Terraform input variables
###############################################################################

variable "aws_region" {
  description = "AWS region for every resource."
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Short prefix used in every resource name."
  type        = string
  default     = "s24-video"
}

variable "environment" {
  description = "Environment tag (dev / demo / prod)."
  type        = string
  default     = "demo"
}

# ----------------------------------------------------------------------------- network
variable "vpc_cidr" {
  description = "CIDR block of the VPC."
  type        = string
  default     = "10.40.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones / public subnets (EKS and MSK need >= 2)."
  type        = number
  default     = 2
}

# ----------------------------------------------------------------------------- EKS
variable "eks_version" {
  description = "Kubernetes version of the EKS control plane."
  type        = string
  default     = "1.31"
}

variable "node_instance_types" {
  description = "EC2 instance types of the managed node group."
  type        = list(string)
  default     = ["t3.large"]
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 5
}

# ----------------------------------------------------------------------------- RDS
variable "db_instance_class" {
  type    = string
  default = "db.t3.micro"
}

variable "db_engine_version" {
  description = "PostgreSQL major (or major.minor) version."
  type        = string
  default     = "16"
}

variable "db_name" {
  type    = string
  default = "s24"
}

variable "db_username" {
  type    = string
  default = "s24admin"
}

# ----------------------------------------------------------------------------- MSK
variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "msk_broker_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

# ----------------------------------------------------------------------------- EMR / Flink
variable "emr_release_label" {
  description = "EMR release that ships Apache Flink 1.18.x."
  type        = string
  default     = "emr-7.1.0"
}

variable "emr_master_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_instance_count" {
  type    = number
  default = 1
}

variable "emr_key_name" {
  description = "Optional EC2 key pair for SSH into the EMR master (null = no SSH)."
  type        = string
  default     = null
}

variable "pyflink_version" {
  description = "apache-flink pip package version installed on EMR nodes (must match EMR Flink minor)."
  type        = string
  default     = "1.18.1"
}

variable "flink_kafka_connector_version" {
  description = "Version of org.apache.flink:flink-sql-connector-kafka downloaded onto EMR nodes."
  type        = string
  default     = "3.2.0-1.18"
}

variable "loki_push_url" {
  description = <<-EOT
    Loki push endpoint reachable from the VPC (internal LB of the loki-gateway service),
    e.g. http://internal-xxxx.ap-south-1.elb.amazonaws.com/loki/api/v1/push.
    Leave empty on the first apply; set it after observability is installed and
    re-apply so the EMR bootstrap ships Flink logs to Loki.
  EOT
  type        = string
  default     = ""
}

# ----------------------------------------------------------------------------- GitOps
variable "gitops_repo_url" {
  description = "HTTPS URL of the Git repository ArgoCD tracks (this repository)."
  type        = string
}

variable "gitops_repo_revision" {
  type    = string
  default = "main"
}

variable "gitops_path" {
  description = "Path inside the repo that ArgoCD syncs."
  type        = string
  default     = "k8s-apps/overlays/aws"
}

variable "gitops_repo_username" {
  description = "Git username for a private repository (leave empty for public repos)."
  type        = string
  default     = ""
}

variable "gitops_repo_token" {
  description = "Git personal-access token for a private repository (leave empty for public repos)."
  type        = string
  default     = ""
  sensitive   = true
}

# ----------------------------------------------------------------------------- Helm chart versions
variable "argocd_chart_version" {
  type    = string
  default = "7.6.12"
}

variable "argocd_apps_chart_version" {
  type    = string
  default = "2.0.2"
}

variable "ingress_nginx_chart_version" {
  type    = string
  default = "4.11.3"
}

# ----------------------------------------------------------------------------- application
variable "service_names" {
  description = "Container images built from services/ (one ECR repository each)."
  type        = list(string)
  default = [
    "user-service",
    "catalog-service",
    "metadata-service",
    "content-delivery-api",
    "cache-manager",
    "progress-tracker",
    "usage-analytics-flink",
  ]
}
