locals {
  nft_contracts_tags = { component = "ingestion-nft-contracts", layer = "bronze" }
}

data "archive_file" "nft_contracts_zip" {
  type        = "zip"
  output_path = "${path.module}/build/nft_contracts.zip"

  source {
    content  = file("${path.module}/../ingestion/nft_contracts/handler.py")
    filename = "ingestion/nft_contracts/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/nft_contracts/validate.py")
    filename = "ingestion/nft_contracts/validate.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/nft_contracts/__init__.py"
  }
}

resource "aws_iam_role" "nft_contracts" {
  name = "load-nft-contracts-role"
  tags = local.nft_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "nft_contracts_s3" {
  name = "landing-read-bronze-write"
  role = aws_iam_role.nft_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/landing/nft_contracts/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/nft_contracts/*"
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
          aws_glue_catalog_table.nft_contracts.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "nft_contracts_logs" {
  role       = aws_iam_role.nft_contracts.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "nft_contracts" {
  name              = "/aws/lambda/load-nft-contracts"
  retention_in_days = 7
  tags              = local.nft_contracts_tags
}

resource "aws_lambda_function" "nft_contracts" {
  function_name = "load-nft-contracts"
  role          = aws_iam_role.nft_contracts.arn
  tags          = local.nft_contracts_tags

  filename         = data.archive_file.nft_contracts_zip.output_path
  source_code_hash = data.archive_file.nft_contracts_zip.output_base64sha256

  handler     = "ingestion.nft_contracts.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn] # defined in lambda_dcl_contracts.tf

  environment {
    variables = {
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.nft_contracts]
}

resource "aws_lambda_permission" "nft_contracts_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.nft_contracts.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lake.arn
}

# NOTE: a bucket supports ONE aws_s3_bucket_notification resource. Future
# landing/ triggers are added as additional lambda_function blocks HERE.
resource "aws_s3_bucket_notification" "lake" {
  bucket = aws_s3_bucket.lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.nft_contracts.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/nft_contracts/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.nft_contracts_s3]
}
