locals {
  run_dbt_tags = { component = "dbt", layer = "silver" }
}

resource "aws_ecr_repository" "run_dbt" {
  name         = "decentraland-run-dbt"
  force_delete = true
  tags         = local.run_dbt_tags
}

# Keep only the 3 most recent images so ECR storage stays near the free tier.
resource "aws_ecr_lifecycle_policy" "run_dbt" {
  repository = aws_ecr_repository.run_dbt.name
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

# Resolves the digest of the pushed :latest tag; a new push + apply
# redeploys the function (image_uri changes with the digest).
data "aws_ecr_image" "run_dbt" {
  repository_name = aws_ecr_repository.run_dbt.name
  image_tag       = "latest"
}

resource "aws_iam_role" "run_dbt" {
  name = "run-dbt-role"
  tags = local.run_dbt_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "run_dbt" {
  name = "dbt-athena-glue-s3-sns"
  role = aws_iam_role.run_dbt.id

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
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
        ]
      },
      # dbt materializes silver views: create/replace/drop table metadata
      {
        Effect = "Allow"
        Action = [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchDeleteTableVersion", # dbt-athena prunes old view versions
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/bronze/contracts/*",
          "${aws_s3_bucket.lake.arn}/bronze/erc20_tokens/*",
          "${aws_s3_bucket.lake.arn}/bronze/token_prices/*",
        ]
      },
      {
        Effect = "Allow"
        # DeleteObject: the table materialization drops the previous
        # build's data files before recreating. Query results stay under
        # athena-results/; table data lives in the medallion prefixes
        # (silver/, gold/) via s3_data_dir in profiles.yml.
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/athena-results/*",
          "${aws_s3_bucket.lake.arn}/silver/*",
          "${aws_s3_bucket.lake.arn}/gold/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "run_dbt_logs" {
  role       = aws_iam_role.run_dbt.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "run_dbt" {
  name              = "/aws/lambda/run-dbt"
  retention_in_days = 7
  tags              = local.run_dbt_tags
}

resource "aws_lambda_function" "run_dbt" {
  function_name = "run-dbt"
  description   = "dbt build for models depending on bronze.contracts"
  role          = aws_iam_role.run_dbt.arn
  tags          = local.run_dbt_tags

  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.run_dbt.repository_url}@${data.aws_ecr_image.run_dbt.image_digest}"
  architectures = ["arm64"] # native build on Apple Silicon, cheaper on Lambda

  timeout     = 900
  memory_size = 1024

  environment {
    variables = {
      DBT_PROJECT_DIR                = "/var/task/dbt"
      DBT_SEND_ANONYMOUS_USAGE_STATS = "False" # avoids ~/.dbt writes on a read-only FS
      HOME                           = "/tmp"
    }
  }

  depends_on = [aws_cloudwatch_log_group.run_dbt]
}
