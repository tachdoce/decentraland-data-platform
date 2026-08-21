locals {
  dcl_contracts_tags = { component = "ingestion-dcl-contracts", layer = "bronze" }
  # From Task 5 Step 1:
  sdk_pandas_layer_arn = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:9"
}

data "archive_file" "dcl_contracts_zip" {
  type        = "zip"
  output_path = "${path.module}/build/dcl_contracts.zip"

  source {
    content  = file("${path.module}/../ingestion/dcl_contracts/handler.py")
    filename = "ingestion/dcl_contracts/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/dcl_contracts/transform.py")
    filename = "ingestion/dcl_contracts/transform.py"
  }
  source {
    content  = ""
    filename = "ingestion/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/dcl_contracts/__init__.py"
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

resource "aws_iam_role" "dcl_contracts" {
  name = "extract-dcl-contracts-role"
  tags = local.dcl_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "dcl_contracts_s3" {
  name = "write-bronze-dcl-contracts"
  role = aws_iam_role.dcl_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/dcl_contracts/*"
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
          aws_glue_catalog_table.dcl_contracts.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dcl_contracts_logs" {
  role       = aws_iam_role.dcl_contracts.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "dcl_contracts" {
  name              = "/aws/lambda/extract-dcl-contracts"
  retention_in_days = 7
  tags              = local.dcl_contracts_tags
}

resource "aws_lambda_function" "dcl_contracts" {
  function_name = "extract-dcl-contracts"
  role          = aws_iam_role.dcl_contracts.arn
  tags          = local.dcl_contracts_tags

  filename         = data.archive_file.dcl_contracts_zip.output_path
  source_code_hash = data.archive_file.dcl_contracts_zip.output_base64sha256

  handler     = "ingestion.dcl_contracts.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn]

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.bucket
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.dcl_contracts]
}
