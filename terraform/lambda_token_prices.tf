locals {
  token_prices_tags = { component = "ingestion-token-prices", layer = "bronze" }
}

data "archive_file" "token_prices_zip" {
  type        = "zip"
  output_path = "${path.module}/build/token_prices.zip"

  source {
    content  = file("${path.module}/../ingestion/token_prices/handler.py")
    filename = "ingestion/token_prices/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/token_prices/defillama.py")
    filename = "ingestion/token_prices/defillama.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/token_prices/__init__.py"
  }
  source {
    content  = file("${path.module}/../ingestion/common/partitions.py")
    filename = "ingestion/common/partitions.py"
  }
  source {
    content  = ""
    filename = "ingestion/common/__init__.py"
  }
}

resource "aws_iam_role" "token_prices" {
  name = "extract-token-prices-role"
  tags = local.token_prices_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "token_prices_s3" {
  name = "prices-write-dim-read"
  role = aws_iam_role.token_prices.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/token_prices/*"
      },
      # Athena reads dimension data and writes query results with the
      # caller's credentials
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/silver/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = aws_athena_workgroup.main.arn
      },
      # Read the silver dimension (dbt-created tables have no Terraform
      # ARN, hence the table wildcard) and register month partitions on
      # the new bronze table.
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
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
          aws_glue_catalog_table.token_prices.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_table.token_prices.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "token_prices_logs" {
  role       = aws_iam_role.token_prices.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "token_prices" {
  name              = "/aws/lambda/extract-token-prices"
  retention_in_days = 7
  tags              = local.token_prices_tags
}

resource "aws_lambda_function" "token_prices" {
  function_name = "extract-token-prices"
  role          = aws_iam_role.token_prices.arn
  tags          = local.token_prices_tags

  filename         = data.archive_file.token_prices_zip.output_path
  source_code_hash = data.archive_file.token_prices_zip.output_base64sha256

  handler     = "ingestion.token_prices.handler.handler"
  runtime     = "python3.13"
  timeout     = 300
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
      LAKE_BUCKET      = aws_s3_bucket.lake.id
    }
  }

  depends_on = [aws_cloudwatch_log_group.token_prices]
}
