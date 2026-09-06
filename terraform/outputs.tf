output "lake_bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "github_actions_dashboard_role_arn" {
  description = "Role the dashboard deploy workflow assumes via OIDC (dash-3)"
  value       = aws_iam_role.github_actions_dashboard.arn
}
