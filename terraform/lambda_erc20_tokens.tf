locals {
  erc20_tokens_tags = { component = "ingestion-erc20-tokens", layer = "bronze" }
}

data "archive_file" "erc20_tokens_zip" {
  type        = "zip"
  output_path = "${path.module}/build/erc20_tokens.zip"

  source {
    content  = file("${path.module}/../ingestion/erc20_tokens/handler.py")
    filename = "ingestion/erc20_tokens/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/erc20_tokens/validate.py")
    filename = "ingestion/erc20_tokens/validate.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/erc20_tokens/__init__.py"
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

resource "aws_iam_role" "erc20_tokens" {
  name = "load-erc20-tokens-role"
  tags = local.erc20_tokens_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "erc20_tokens_s3" {
  name = "landing-read-bronze-write"
  role = aws_iam_role.erc20_tokens.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/landing/erc20_tokens/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/erc20_tokens/*"
      },
      # Athena writes DDL query results with the caller's credentials
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      # Run ALTER TABLE ADD PARTITION in the tagged workgroup
      {
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution"]
        Resource = aws_athena_workgroup.main.arn
      },
      # Athena DDL resolves the table and creates the partition through Glue
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_table.erc20_tokens.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "erc20_tokens_logs" {
  role       = aws_iam_role.erc20_tokens.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "erc20_tokens" {
  name              = "/aws/lambda/load-erc20-tokens"
  retention_in_days = 7
  tags              = local.erc20_tokens_tags
}

resource "aws_lambda_function" "erc20_tokens" {
  function_name = "load-erc20-tokens"
  role          = aws_iam_role.erc20_tokens.arn
  tags          = local.erc20_tokens_tags

  filename         = data.archive_file.erc20_tokens_zip.output_path
  source_code_hash = data.archive_file.erc20_tokens_zip.output_base64sha256

  handler     = "ingestion.erc20_tokens.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.erc20_tokens]
}
