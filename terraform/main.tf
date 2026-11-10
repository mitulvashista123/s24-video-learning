###############################################################################
# S24 – Online Video Learning :: complete AWS infrastructure (Terraform)
#
#   network      : VPC, 2 public subnets, IGW, route table, S3/DynamoDB gateway
#                  endpoints, Lambda/STS interface endpoints
#   compute      : EKS cluster + managed node group + addons (vpc-cni, coredns,
#                  kube-proxy, metrics-server) + IRSA
#   storage (3)  : RDS PostgreSQL (SQL), DynamoDB (NoSQL), S3 (object)
#   messaging    : Amazon MSK (Kafka), SQS dead-letter queue
#   serverless   : Lambda recommendation-fn (S3 trigger + MSK trigger)
#   streaming    : EMR cluster running the PyFlink usage-analytics job
#   registry     : ECR repository per container image
#   platform     : ingress-nginx, ArgoCD + root Application (GitOps),
#                  namespace / IRSA service account / config & secrets
###############################################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.tags
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  name         = var.project_name
  cluster_name = "${var.project_name}-eks"
  azs          = slice(data.aws_availability_zones.available.names, 0, var.az_count)
  suffix       = random_id.suffix.hex
  account_id   = data.aws_caller_identity.current.account_id
  partition    = data.aws_partition.current.partition

  app_namespace       = "s24"
  app_service_account = "s24-app"

  kafka_topics = {
    progress   = "learner-progress-events"
    access     = "content-access-events"
    results    = "usage-analytics-results"
    invalidate = "cache-invalidation"
  }

  tags = {
    Project     = "S24-Online-Video-Learning"
    Course      = "CS-SS-G527"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

###############################################################################
# 1. NETWORK
###############################################################################

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${local.name}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name}-igw" }
}

resource "aws_subnet" "public" {
  count                   = var.az_count
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name                                          = "${local.name}-public-${local.azs[count.index]}"
    "kubernetes.io/role/elb"                      = "1"
    "kubernetes.io/role/internal-elb"             = "1"
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${local.name}-public-rt" }
}

resource "aws_route_table_association" "public" {
  count          = var.az_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Gateway endpoints: Lambda in the VPC (no public IP) reaches S3 and DynamoDB privately.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
  tags              = { Name = "${local.name}-s3-endpoint" }
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
  tags              = { Name = "${local.name}-dynamodb-endpoint" }
}

resource "aws_security_group" "endpoints" {
  name        = "${local.name}-vpce"
  description = "HTTPS from inside the VPC to interface endpoints"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Interface endpoints required by the Lambda MSK event-source poller (no NAT gateway here).
resource "aws_vpc_endpoint" "interface" {
  for_each            = toset(["lambda", "sts"])
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.public[*].id
  security_group_ids  = [aws_security_group.endpoints.id]
  private_dns_enabled = true
  tags                = { Name = "${local.name}-${each.key}-endpoint" }
}

###############################################################################
# 2. SECURITY GROUPS
###############################################################################

resource "aws_security_group" "rds" {
  name        = "${local.name}-rds"
  description = "PostgreSQL from inside the VPC only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "msk" {
  name        = "${local.name}-msk"
  description = "Kafka brokers"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Kafka plaintext / TLS / IAM from the VPC"
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "Self (required by the Lambda MSK event-source mapping)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "lambda" {
  name        = "${local.name}-lambda"
  description = "Recommendation Lambda"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "emr_master" {
  name                   = "${local.name}-emr-master"
  description            = "EMR managed master SG (rules managed by EMR)"
  vpc_id                 = aws_vpc.main.id
  revoke_rules_on_delete = true

  lifecycle {
    ignore_changes = [ingress, egress]
  }
}

resource "aws_security_group" "emr_core" {
  name                   = "${local.name}-emr-core"
  description            = "EMR managed core SG (rules managed by EMR)"
  vpc_id                 = aws_vpc.main.id
  revoke_rules_on_delete = true

  lifecycle {
    ignore_changes = [ingress, egress]
  }
}

# EMR adds the ingress rules itself but refuses to launch into a managed SG without
# an egress rule (Terraform strips AWS's default allow-all egress from new groups).
resource "aws_vpc_security_group_egress_rule" "emr_all" {
  for_each          = { master = aws_security_group.emr_master.id, core = aws_security_group.emr_core.id }
  security_group_id = each.value
  description       = "All outbound (S3, MSK, package repos)"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

###############################################################################
# 3. EKS
###############################################################################

data "aws_iam_policy_document" "eks_cluster_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_cluster" {
  name               = "${local.name}-eks-cluster-role"
  assume_role_policy = data.aws_iam_policy_document.eks_cluster_assume.json
}

resource "aws_iam_role_policy_attachment" "eks_cluster" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "main" {
  name     = local.cluster_name
  version  = var.eks_version
  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {
    subnet_ids              = aws_subnet.public[*].id
    endpoint_public_access  = true
    endpoint_private_access = true
  }

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  enabled_cluster_log_types = ["api", "audit"]

  depends_on = [aws_iam_role_policy_attachment.eks_cluster]
}

data "aws_iam_policy_document" "eks_node_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_node" {
  name               = "${local.name}-eks-node-role"
  assume_role_policy = data.aws_iam_policy_document.eks_node_assume.json
}

resource "aws_iam_role_policy_attachment" "eks_node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy",
    "AmazonEKS_CNI_Policy",
    "AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.key}"
}

resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${local.name}-ng"
  node_role_arn   = aws_iam_role.eks_node.arn
  subnet_ids      = aws_subnet.public[*].id
  instance_types  = var.node_instance_types
  ami_type        = "AL2023_x86_64_STANDARD"
  capacity_type   = "ON_DEMAND"
  disk_size       = 40

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    workload = "s24"
  }

  depends_on = [aws_iam_role_policy_attachment.eks_node]

  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }
}

resource "aws_eks_addon" "core" {
  for_each                    = toset(["vpc-cni", "kube-proxy", "coredns", "metrics-server"])
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = each.key
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.main]
}

# ----------------------------------------------------------------------------- IRSA
data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint]
}

locals {
  oidc_host = replace(aws_eks_cluster.main.identity[0].oidc[0].issuer, "https://", "")
}

data "aws_iam_policy_document" "app_irsa_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:sub"
      values   = ["system:serviceaccount:${local.app_namespace}:${local.app_service_account}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "app_irsa" {
  name               = "${local.name}-app-irsa"
  assume_role_policy = data.aws_iam_policy_document.app_irsa_assume.json
}

data "aws_iam_policy_document" "app_access" {
  statement {
    sid = "DynamoDB"
    actions = [
      "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
      "dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem",
      "dynamodb:DescribeTable",
    ]
    resources = flatten([
      for t in local.dynamodb_tables : [t, "${t}/index/*"]
    ])
  }

  statement {
    sid       = "S3Objects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.assets.arn}/*"]
  }

  statement {
    sid       = "S3Bucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.assets.arn]
  }
}

resource "aws_iam_role_policy" "app_access" {
  name   = "${local.name}-app-access"
  role   = aws_iam_role.app_irsa.id
  policy = data.aws_iam_policy_document.app_access.json
}

###############################################################################
# 4. CONTAINER REGISTRY
###############################################################################

resource "aws_ecr_repository" "services" {
  for_each             = toset(var.service_names)
  name                 = "${var.project_name}/${each.key}"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "services" {
  for_each   = aws_ecr_repository.services
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 20 most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}

###############################################################################
# 5. STORAGE #1 – RDS PostgreSQL (relational: learners, courses, lessons, video metadata)
###############################################################################

resource "random_password" "db" {
  length           = 24
  special          = true
  override_special = "-_=+"
}

resource "aws_db_subnet_group" "main" {
  name       = "${local.name}-db-subnets"
  subnet_ids = aws_subnet.public[*].id
}

resource "aws_db_instance" "postgres" {
  identifier                 = "${local.name}-postgres"
  engine                     = "postgres"
  engine_version             = var.db_engine_version
  instance_class             = var.db_instance_class
  allocated_storage          = 20
  max_allocated_storage      = 50
  storage_type               = "gp3"
  storage_encrypted          = true
  db_name                    = var.db_name
  username                   = var.db_username
  password                   = random_password.db.result
  port                       = 5432
  db_subnet_group_name       = aws_db_subnet_group.main.name
  vpc_security_group_ids     = [aws_security_group.rds.id]
  publicly_accessible        = false
  multi_az                   = false
  backup_retention_period    = 1
  auto_minor_version_upgrade = true
  deletion_protection        = false
  skip_final_snapshot        = true
  apply_immediately          = true
}

###############################################################################
# 6. STORAGE #2 – DynamoDB (NoSQL: learner progress, popularity, recommendations, analytics)
###############################################################################

resource "aws_dynamodb_table" "learner_progress" {
  name         = "LearnerProgress"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "UserID"
  range_key    = "LessonID"

  attribute {
    name = "UserID"
    type = "S"
  }

  attribute {
    name = "LessonID"
    type = "S"
  }

  global_secondary_index {
    name            = "LessonIndex"
    hash_key        = "LessonID"
    range_key       = "UserID"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }
}

resource "aws_dynamodb_table" "lesson_popularity" {
  name         = "LessonPopularity"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LessonID"

  attribute {
    name = "LessonID"
    type = "S"
  }
}

resource "aws_dynamodb_table" "recommendations" {
  name         = "Recommendations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "Scope"

  attribute {
    name = "Scope"
    type = "S"
  }
}

resource "aws_dynamodb_table" "usage_analytics" {
  name         = "UsageAnalytics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ItemID"
  range_key    = "WindowStart"

  attribute {
    name = "ItemID"
    type = "S"
  }

  attribute {
    name = "WindowStart"
    type = "S"
  }

  ttl {
    attribute_name = "ExpiresAt"
    enabled        = true
  }
}

locals {
  dynamodb_tables = [
    aws_dynamodb_table.learner_progress.arn,
    aws_dynamodb_table.lesson_popularity.arn,
    aws_dynamodb_table.recommendations.arn,
    aws_dynamodb_table.usage_analytics.arn,
  ]
}

###############################################################################
# 7. STORAGE #3 – S3 (object: videos, subtitles) + artifacts bucket (EMR job, logs)
###############################################################################

resource "aws_s3_bucket" "assets" {
  bucket        = "${local.name}-video-assets-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "assets" {
  bucket                  = aws_s3_bucket.assets.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "assets" {
  bucket = aws_s3_bucket.assets.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "assets" {
  bucket = aws_s3_bucket.assets.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_cors_configuration" "assets" {
  bucket = aws_s3_bucket.assets.id
  cors_rule {
    allowed_methods = ["GET", "PUT", "HEAD"]
    allowed_origins = ["*"]
    allowed_headers = ["*"]
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "assets" {
  bucket = aws_s3_bucket.assets.id
  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.name}-artifacts-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "flink_job" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "emr/flink_job.py"
  source = "${path.module}/../services/usage-analytics-flink/flink_job.py"
  etag   = filemd5("${path.module}/../services/usage-analytics-flink/flink_job.py")
}

resource "aws_s3_object" "emr_bootstrap" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "emr/bootstrap.sh"
  source = "${path.module}/scripts/emr-bootstrap.sh"
  etag   = filemd5("${path.module}/scripts/emr-bootstrap.sh")
}

###############################################################################
# 8. MESSAGING – Amazon MSK (Kafka) + SQS DLQ
###############################################################################

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 7
}

resource "aws_msk_configuration" "main" {
  name           = "${local.name}-kafka-config"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=6
    log.retention.hours=72
    delete.topic.enable=true
  PROPERTIES
}

resource "aws_msk_cluster" "main" {
  cluster_name           = "${local.name}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.az_count

  broker_node_group_info {
    instance_type   = var.msk_broker_instance_type
    client_subnets  = aws_subnet.public[*].id
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

resource "aws_sqs_queue" "recommendation_dlq" {
  name                      = "${local.name}-recommendation-dlq"
  message_retention_seconds = 1209600
}

###############################################################################
# 9. SERVERLESS – Lambda recommendation-fn (Service 7)
###############################################################################

data "archive_file" "recommendation_fn" {
  type        = "zip"
  source_dir  = "${path.module}/../services/recommendation-fn"
  output_path = "${path.module}/.build/recommendation-fn.zip"
  excludes    = ["__pycache__", "tests"]
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "recommendation_fn" {
  name               = "${local.name}-recommendation-fn"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "recommendation_fn_managed" {
  for_each = toset([
    "service-role/AWSLambdaBasicExecutionRole",
    "service-role/AWSLambdaVPCAccessExecutionRole",
    "service-role/AWSLambdaMSKExecutionRole",
  ])
  role       = aws_iam_role.recommendation_fn.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.key}"
}

data "aws_iam_policy_document" "recommendation_fn" {
  statement {
    sid = "DynamoDB"
    actions = [
      "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
      "dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchWriteItem",
    ]
    resources = local.dynamodb_tables
  }

  statement {
    sid       = "ReadUploadedObjects"
    actions   = ["s3:GetObject", "s3:GetObjectTagging"]
    resources = ["${aws_s3_bucket.assets.arn}/*"]
  }

  statement {
    sid       = "DeadLetterQueue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.recommendation_dlq.arn]
  }

  statement {
    sid = "KafkaCluster"
    actions = [
      "kafka:DescribeCluster", "kafka:DescribeClusterV2",
      "kafka:GetBootstrapBrokers", "kafka-cluster:Connect",
      "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "recommendation_fn" {
  name   = "${local.name}-recommendation-fn"
  role   = aws_iam_role.recommendation_fn.id
  policy = data.aws_iam_policy_document.recommendation_fn.json
}

resource "aws_cloudwatch_log_group" "recommendation_fn" {
  name              = "/aws/lambda/${local.name}-recommendation-fn"
  retention_in_days = 7
}

resource "aws_lambda_function" "recommendation_fn" {
  function_name    = "${local.name}-recommendation-fn"
  role             = aws_iam_role.recommendation_fn.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.recommendation_fn.output_path
  source_code_hash = data.archive_file.recommendation_fn.output_base64sha256
  timeout          = 60
  memory_size      = 256

  vpc_config {
    subnet_ids         = aws_subnet.public[*].id
    security_group_ids = [aws_security_group.lambda.id, aws_security_group.msk.id]
  }

  environment {
    variables = {
      POPULARITY_TABLE      = aws_dynamodb_table.lesson_popularity.name
      RECOMMENDATIONS_TABLE = aws_dynamodb_table.recommendations.name
      ANALYTICS_TABLE       = aws_dynamodb_table.usage_analytics.name
      VIDEO_PREFIX          = "videos/"
      TOP_N                 = "20"
      SCORE_HALF_LIFE_HOURS = "24"
      NEW_CONTENT_BOOST     = "5"
      ANALYTICS_TTL_DAYS    = "7"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.recommendation_fn,
    aws_iam_role_policy_attachment.recommendation_fn_managed,
  ]
}

# Asynchronous invocations (S3 events) that fail twice go to the SQS dead-letter queue.
resource "aws_lambda_function_event_invoke_config" "recommendation_fn" {
  function_name                = aws_lambda_function.recommendation_fn.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 3600

  destination_config {
    on_failure {
      destination = aws_sqs_queue.recommendation_dlq.arn
    }
  }
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.recommendation_fn.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.assets.arn
}

# Trigger #1: video upload to S3 (asynchronous, event-driven)
resource "aws_s3_bucket_notification" "assets" {
  bucket = aws_s3_bucket.assets.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.recommendation_fn.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "videos/"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

# Trigger #2: Flink window aggregates on the results topic
resource "aws_lambda_event_source_mapping" "analytics_results" {
  function_name     = aws_lambda_function.recommendation_fn.arn
  event_source_arn  = aws_msk_cluster.main.arn
  topics            = [local.kafka_topics.results]
  starting_position = "LATEST"
  batch_size        = 100
  enabled           = true

  amazon_managed_kafka_event_source_config {
    consumer_group_id = "recommendation-fn"
  }

  depends_on = [
    aws_vpc_endpoint.interface,
    aws_iam_role_policy.recommendation_fn,
  ]
}

###############################################################################
# 10. STREAM PROCESSING – EMR (Apache Flink) running the usage-analytics job (Service 8)
###############################################################################

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_service" {
  name               = "${local.name}-emr-service"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2" {
  name               = "${local.name}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.eks_node_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${local.name}-emr-ec2"
  role = aws_iam_role.emr_ec2.name
}

locals {
  flink_submit = join(" ", [
    "set -euo pipefail;",
    "aws s3 cp s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.flink_job.key} /home/hadoop/flink_job.py;",
    "flink run -t yarn-per-job --detached",
    "-Dyarn.application.name=s24-usage-analytics",
    "-Djobmanager.memory.process.size=1600m",
    "-Dtaskmanager.memory.process.size=2560m",
    "-Dtaskmanager.numberOfTaskSlots=2",
    "-pyclientexec /opt/s24/pyflink/bin/python -pyexec /opt/s24/pyflink/bin/python",
    "-py /home/hadoop/flink_job.py",
    "--bootstrap-servers ${aws_msk_cluster.main.bootstrap_brokers}",
    "--source-topics ${local.kafka_topics.progress},${local.kafka_topics.access}",
    "--sink-topic ${local.kafka_topics.results}",
    "--group-id s24-usage-analytics",
  ])
}

resource "aws_emr_cluster" "flink" {
  name                              = "${local.name}-flink"
  release_label                     = var.emr_release_label
  applications                      = ["Hadoop", "Flink"]
  service_role                      = aws_iam_role.emr_service.arn
  log_uri                           = "s3://${aws_s3_bucket.artifacts.id}/emr-logs/"
  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  ec2_attributes {
    subnet_id                         = aws_subnet.public[0].id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    key_name                          = var.emr_key_name
  }

  master_instance_group {
    instance_type = var.emr_master_instance_type
  }

  # Omitted when emr_core_instance_count = 0: a single-node cluster runs YARN on the master.
  dynamic "core_instance_group" {
    for_each = var.emr_core_instance_count > 0 ? [1] : []
    content {
      instance_type  = var.emr_core_instance_type
      instance_count = var.emr_core_instance_count

      ebs_config {
        size                 = 32
        type                 = "gp3"
        volumes_per_instance = 1
      }
    }
  }

  bootstrap_action {
    name = "install-pyflink-kafka-connector-and-log-shipper"
    path = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.emr_bootstrap.key}"
    args = [
      var.pyflink_version,
      var.flink_kafka_connector_version,
      var.loki_push_url == "" ? "none" : var.loki_push_url,
    ]
  }

  configurations_json = jsonencode([
    {
      Classification = "flink-conf"
      Properties = {
        "state.backend.type"                    = "hashmap"
        "state.checkpoints.dir"                 = "s3://${aws_s3_bucket.artifacts.id}/flink-checkpoints/"
        "execution.checkpointing.interval"      = "30s"
        "execution.checkpointing.mode"          = "EXACTLY_ONCE"
        "restart-strategy.type"                 = "fixed-delay"
        "restart-strategy.fixed-delay.attempts" = "10"
        "restart-strategy.fixed-delay.delay"    = "15s"
      }
    }
  ])

  step {
    name              = "submit-usage-analytics-flink-job"
    action_on_failure = "CONTINUE"

    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = ["bash", "-c", local.flink_submit]
    }
  }

  depends_on = [
    aws_msk_cluster.main,
    aws_iam_role_policy_attachment.emr_service,
    aws_iam_role_policy_attachment.emr_ec2,
    aws_route_table_association.public,
    aws_vpc_security_group_egress_rule.emr_all,
  ]

  lifecycle {
    ignore_changes = [step]
  }
}

###############################################################################
# 11. KUBERNETES PLATFORM (namespace, IRSA SA, config, ingress-nginx, ArgoCD)
###############################################################################

provider "kubernetes" {
  host                   = aws_eks_cluster.main.endpoint
  cluster_ca_certificate = base64decode(aws_eks_cluster.main.certificate_authority[0].data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", aws_eks_cluster.main.name, "--region", var.aws_region]
  }
}

provider "helm" {
  kubernetes {
    host                   = aws_eks_cluster.main.endpoint
    cluster_ca_certificate = base64decode(aws_eks_cluster.main.certificate_authority[0].data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", aws_eks_cluster.main.name, "--region", var.aws_region]
    }
  }
}

resource "kubernetes_namespace" "app" {
  metadata {
    name = local.app_namespace
    labels = {
      "app.kubernetes.io/part-of" = "s24-video-learning"
    }
  }
  depends_on = [aws_eks_node_group.main]
}

resource "kubernetes_service_account" "app" {
  metadata {
    name      = local.app_service_account
    namespace = kubernetes_namespace.app.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.app_irsa.arn
    }
  }
}

# Cloud endpoints produced by Terraform are handed to the GitOps-managed workloads here,
# so the manifests in Git never contain environment-specific values.
resource "kubernetes_config_map" "app" {
  metadata {
    name      = "s24-config"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    AWS_REGION                = var.aws_region
    AWS_DEFAULT_REGION        = var.aws_region
    DB_HOST                   = aws_db_instance.postgres.address
    DB_PORT                   = tostring(aws_db_instance.postgres.port)
    DB_NAME                   = var.db_name
    DB_SSLMODE                = "require"
    S3_BUCKET                 = aws_s3_bucket.assets.id
    PROGRESS_TABLE            = aws_dynamodb_table.learner_progress.name
    POPULARITY_TABLE          = aws_dynamodb_table.lesson_popularity.name
    RECOMMENDATIONS_TABLE     = aws_dynamodb_table.recommendations.name
    ANALYTICS_TABLE           = aws_dynamodb_table.usage_analytics.name
    KAFKA_BOOTSTRAP_SERVERS   = aws_msk_cluster.main.bootstrap_brokers
    KAFKA_REPLICATION_FACTOR  = "2"
    KAFKA_PARTITIONS          = "6"
    TOPIC_PROGRESS_EVENTS     = local.kafka_topics.progress
    TOPIC_ACCESS_EVENTS       = local.kafka_topics.access
    TOPIC_ANALYTICS_RESULTS   = local.kafka_topics.results
    TOPIC_CACHE_INVALIDATION  = local.kafka_topics.invalidate
    USER_SERVICE_URL          = "http://user-service.${local.app_namespace}.svc.cluster.local"
    CATALOG_SERVICE_URL       = "http://catalog-service.${local.app_namespace}.svc.cluster.local"
    METADATA_SERVICE_URL      = "http://metadata-service.${local.app_namespace}.svc.cluster.local"
    PROGRESS_SERVICE_URL      = "http://progress-tracker.${local.app_namespace}.svc.cluster.local"
    CACHE_MANAGER_HTTP_URL    = "http://cache-manager.${local.app_namespace}.svc.cluster.local"
    CACHE_MANAGER_GRPC_TARGET = "dns:///cache-manager-headless.${local.app_namespace}.svc.cluster.local:50051"
  }
}

resource "kubernetes_secret" "db" {
  metadata {
    name      = "s24-db"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    DB_USER     = var.db_username
    DB_PASSWORD = random_password.db.result
  }

  type = "Opaque"
}

resource "helm_release" "ingress_nginx" {
  name = "ingress-nginx"
  # Vendored: packaged from the helm-chart-<version> git tag, because GitHub release
  # downloads (where this chart is hosted) are unreliable from some networks.
  chart            = "${path.module}/charts/ingress-nginx-${var.ingress_nginx_chart_version}.tgz"
  namespace        = "ingress-nginx"
  create_namespace = true
  wait             = true
  timeout          = 900

  values = [yamlencode({
    controller = {
      replicaCount = 2
      service = {
        type = "LoadBalancer"
        # Classic ELB: this account is not yet allowed to create NLB/ALB
        # ("This AWS account currently does not support creating load balancers").
        annotations = {
          "service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled" = "true"
        }
      }
      config = {
        "proxy-body-size"       = "512m"
        "use-forwarded-headers" = "true"
      }
      metrics = {
        enabled = true
      }
    }
  })]

  depends_on = [aws_eks_addon.core]
}

resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "oci://ghcr.io/argoproj/argo-helm" # OCI: avoids GitHub release downloads
  chart            = "argo-cd"
  version          = var.argocd_chart_version
  namespace        = "argocd"
  create_namespace = true
  wait             = true
  timeout          = 900

  values = [yamlencode({
    configs = {
      params = {
        "server.insecure" = true
      }
      cm = {
        "timeout.reconciliation" = "60s"
      }
    }
    server = {
      service = {
        type = "LoadBalancer"
      }
    }
  })]

  depends_on = [aws_eks_addon.core]
}

resource "kubernetes_secret" "argocd_repo" {
  count = var.gitops_repo_token == "" ? 0 : 1

  metadata {
    name      = "s24-gitops-repo"
    namespace = "argocd"
    labels = {
      "argocd.argoproj.io/secret-type" = "repository"
    }
  }

  data = {
    type     = "git"
    url      = var.gitops_repo_url
    username = var.gitops_repo_username
    password = var.gitops_repo_token
  }

  depends_on = [helm_release.argocd]
}

# Root ArgoCD Application (same spec as argocd-app.yaml in the repo root).
resource "helm_release" "argocd_root_app" {
  name       = "s24-argocd-apps"
  repository = "oci://ghcr.io/argoproj/argo-helm"
  chart      = "argocd-apps"
  version    = var.argocd_apps_chart_version
  namespace  = "argocd"

  values = [yamlencode({
    applications = {
      "s24-services" = {
        namespace  = "argocd"
        project    = "default"
        finalizers = ["resources-finalizer.argocd.argoproj.io"]
        source = {
          repoURL        = var.gitops_repo_url
          targetRevision = var.gitops_repo_revision
          path           = var.gitops_path
        }
        destination = {
          server    = "https://kubernetes.default.svc"
          namespace = local.app_namespace
        }
        syncPolicy = {
          automated = {
            prune    = true
            selfHeal = true
          }
          syncOptions = ["CreateNamespace=true", "ApplyOutOfSyncOnly=true"]
          retry = {
            limit = 10
            backoff = {
              duration    = "15s"
              factor      = 2
              maxDuration = "5m"
            }
          }
        }
        ignoreDifferences = [{
          group        = "apps"
          kind         = "Deployment"
          jsonPointers = ["/spec/replicas"]
        }]
      }
    }
  })]

  depends_on = [
    helm_release.argocd,
    kubernetes_config_map.app,
    kubernetes_secret.db,
    kubernetes_service_account.app,
    kubernetes_secret.argocd_repo,
  ]
}
