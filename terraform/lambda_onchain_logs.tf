locals {
  onchain_logs_tags = { component = "ingestion-onchain", layer = "bronze" }
  # Created manually (aws ssm put-parameter) so the secret never enters
  # tfstate; Terraform only references the ARN for IAM.
  gcp_key_param_arn = "arn:aws:ssm:us-east-1:${data.aws_caller_identity.current.account_id}:parameter/decentraland/gcp/bq-service-account-key"
}

resource "aws_ecr_repository" "onchain_logs" {
  name         = "decentraland-extract-onchain-logs"
  force_delete = true
  tags         = local.onchain_logs_tags
}

resource "aws_ecr_lifecycle_policy" "onchain_logs" {
  repository = aws_ecr_repository.onchain_logs.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 3 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}

data "aws_ecr_image" "onchain_logs" {
  repository_name = aws_ecr_repository.onchain_logs.name
  image_tag       = "latest"
}

resource "aws_iam_role" "onchain_logs" {
  name = "extract-onchain-logs-role"
  tags = local.onchain_logs_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "onchain_logs" {
  name = "athena-glue-s3-ssm"
  role = aws_iam_role.onchain_logs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = aws_athena_workgroup.main.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
        ]
      },
      # Explicit partition registration (ALTER TABLE ADD PARTITION)
      {
        Effect = "Allow"
        Action = ["glue:CreatePartition", "glue:BatchCreatePartition"]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/ethereum_logs",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/polygon_logs",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      # Append-only by design: PutObject only, no DeleteObject anywhere.
      {
        Effect = "Allow"
        Action = "s3:PutObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/bronze/ethereum_logs/*",
          "${aws_s3_bucket.lake.arn}/bronze/polygon_logs/*",
        ]
      },
      # Athena result staging; also where dbt materializes dim_contracts.
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      {
        Effect   = "Allow"
        Action   = "ssm:GetParameter"
        Resource = local.gcp_key_param_arn
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "onchain_logs_logs" {
  role       = aws_iam_role.onchain_logs.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "onchain_logs" {
  name              = "/aws/lambda/extract-onchain-logs"
  retention_in_days = 7
  tags              = local.onchain_logs_tags
}

resource "aws_lambda_function" "onchain_logs" {
  function_name = "extract-onchain-logs"
  description   = "One day + one chain of Decentraland logs: BigQuery -> bronze"
  role          = aws_iam_role.onchain_logs.arn
  tags          = local.onchain_logs_tags

  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.onchain_logs.repository_url}@${data.aws_ecr_image.onchain_logs.image_digest}"
  architectures = ["arm64"]

  timeout     = 180
  memory_size = 3008

  environment {
    variables = {
      LAKE_BUCKET      = local.bucket_name
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
      GCP_KEY_PARAM    = "/decentraland/gcp/bq-service-account-key"
    }
  }

  depends_on = [aws_cloudwatch_log_group.onchain_logs]
}
