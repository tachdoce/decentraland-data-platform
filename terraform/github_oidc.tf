# GitHub Actions OIDC federation: workflows in this repo assume short-lived
# AWS credentials via a signed GitHub token — no stored access keys.
# First consumer: the dashboard build (dash-3) reading gold/ from S3.

locals {
  github_repo      = "tachdoce/decentraland-data-platform"
  github_oidc_tags = { component = "ci", layer = "ops" }
}

# Registers GitHub's token issuer as a trusted identity provider in the
# account. One per account; future workflows (e.g. terraform deploy on main)
# reuse it. AWS validates GitHub's certificates against trusted roots, but
# the resource still requires the historical thumbprint values.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
  tags = local.github_oidc_tags
}

# Role the dashboard workflow assumes. The trust policy is the security
# boundary: only tokens whose `sub` claim matches this exact repo are
# accepted — a fork or any other repo gets denied at AssumeRole time.
resource "aws_iam_role" "github_actions_dashboard" {
  name = "github-actions-dashboard"
  tags = local.github_oidc_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${local.github_repo}:*"
          }
        }
      }
    ]
  })
}

# Read-only, gold-only: the dashboard build embeds aggregated extracts, so a
# compromised runner could at worst read data the site already publishes.
resource "aws_iam_role_policy" "github_actions_dashboard_read_gold" {
  name = "read-gold"
  role = aws_iam_role.github_actions_dashboard.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListGoldPrefix"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.lake.arn
        Condition = {
          StringLike = { "s3:prefix" = "gold/*" }
        }
      },
      {
        Sid      = "ReadGoldObjects"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/gold/*"
      }
    ]
  })
}
