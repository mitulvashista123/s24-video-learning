output "aws_region" {
  value = var.aws_region
}

output "account_id" {
  value = local.account_id
}

output "eks_cluster_name" {
  value = aws_eks_cluster.main.name
}

output "kubeconfig_command" {
  description = "Run this to point kubectl/helm at the cluster."
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${aws_eks_cluster.main.name}"
}

output "rds_endpoint" {
  value = aws_db_instance.postgres.address
}

output "db_password" {
  value     = random_password.db.result
  sensitive = true
}

output "video_assets_bucket" {
  value = aws_s3_bucket.assets.id
}

output "artifacts_bucket" {
  value = aws_s3_bucket.artifacts.id
}

output "dynamodb_tables" {
  value = {
    learner_progress  = aws_dynamodb_table.learner_progress.name
    lesson_popularity = aws_dynamodb_table.lesson_popularity.name
    recommendations   = aws_dynamodb_table.recommendations.name
    usage_analytics   = aws_dynamodb_table.usage_analytics.name
  }
}

output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.main.bootstrap_brokers
}

output "ecr_registry" {
  value = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

output "ecr_repositories" {
  value = { for k, r in aws_ecr_repository.services : k => r.repository_url }
}

output "recommendation_lambda" {
  value = aws_lambda_function.recommendation_fn.function_name
}

output "recommendation_dlq_url" {
  value = aws_sqs_queue.recommendation_dlq.url
}

output "emr_cluster_id" {
  value = aws_emr_cluster.flink.id
}

output "emr_master_dns" {
  value = aws_emr_cluster.flink.master_public_dns
}

output "app_irsa_role_arn" {
  value = aws_iam_role.app_irsa.arn
}

output "public_url_command" {
  description = "Prints the public URL of the Content Delivery API (ingress NLB)."
  value       = "kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'"
}

output "argocd_url_command" {
  value = "kubectl -n argocd get svc argocd-server -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'"
}

output "argocd_admin_password_command" {
  value = "kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
}
