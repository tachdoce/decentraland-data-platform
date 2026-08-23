locals {
  contracts_tags = { component = "ingestion-contracts", layer = "bronze" }
}

data "archive_file" "contracts_zip" {
  type        = "zip"
  output_path = "${path.module}/build/contracts.zip"

  source {
    content  = file("${path.module}/../ingestion/contracts/handler.py")
    filename = "ingestion/contracts/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/contracts/validate.py")
    filename = "ingestion/contracts/validate.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/contracts/__init__.py"
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

resource "aws_iam_role" "contracts" {
  name = "load-contracts-role"
  tags = local.contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "contracts_s3" {
  name = "landing-read-bronze-write"
  role = aws_iam_role.contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/landing/contracts/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/contracts/*"
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
          aws_glue_catalog_table.contracts.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "contracts_logs" {
  role       = aws_iam_role.contracts.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "contracts" {
  name              = "/aws/lambda/load-contracts"
  retention_in_days = 7
  tags              = local.contracts_tags
}

resource "aws_lambda_function" "contracts" {
  function_name = "load-contracts"
  role          = aws_iam_role.contracts.arn
  tags          = local.contracts_tags

  filename         = data.archive_file.contracts_zip.output_path
  source_code_hash = data.archive_file.contracts_zip.output_base64sha256

  handler     = "ingestion.contracts.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.contracts]
}

# S3 events now flow through EventBridge: the contracts-on-push state
# machine (step_functions_contracts.tf) starts on landing/contracts/*.csv.
# Future landing/ triggers add EventBridge rules, not lambda_function blocks.
resource "aws_s3_bucket_notification" "lake" {
  bucket      = aws_s3_bucket.lake.id
  eventbridge = true
}
