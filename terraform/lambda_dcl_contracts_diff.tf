locals {
  dcl_contracts_diff_tags = { component = "ingestion-dcl-contracts", layer = "bronze" }
}

data "archive_file" "dcl_contracts_diff_zip" {
  type        = "zip"
  output_path = "${path.module}/build/dcl_contracts_diff.zip"

  source {
    content  = file("${path.module}/../ingestion/dcl_contracts_diff/handler.py")
    filename = "ingestion/dcl_contracts_diff/handler.py"
  }
  source {
    content  = file("${path.module}/../ingestion/dcl_contracts_diff/transform.py")
    filename = "ingestion/dcl_contracts_diff/transform.py"
  }
  # partition_key lives in the extractor's transform module
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
    filename = "ingestion/dcl_contracts_diff/__init__.py"
  }
  source {
    content  = ""
    filename = "ingestion/dcl_contracts/__init__.py"
  }
}

resource "aws_iam_role" "dcl_contracts_diff" {
  name = "diff-dcl-contracts-role"
  tags = local.dcl_contracts_diff_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "dcl_contracts_diff" {
  name = "read-snapshots-publish-alerts"
  role = aws_iam_role.dcl_contracts_diff.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/dcl_contracts/*"
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dcl_contracts_diff_logs" {
  role       = aws_iam_role.dcl_contracts_diff.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "dcl_contracts_diff" {
  name              = "/aws/lambda/diff-dcl-contracts"
  retention_in_days = 7
  tags              = local.dcl_contracts_diff_tags
}

resource "aws_lambda_function" "dcl_contracts_diff" {
  function_name = "diff-dcl-contracts"
  role          = aws_iam_role.dcl_contracts_diff.arn
  tags          = local.dcl_contracts_diff_tags

  filename         = data.archive_file.dcl_contracts_diff_zip.output_path
  source_code_hash = data.archive_file.dcl_contracts_diff_zip.output_base64sha256

  handler     = "ingestion.dcl_contracts_diff.handler.handler"
  runtime     = "python3.13"
  timeout     = 60
  memory_size = 512
  layers      = [local.sdk_pandas_layer_arn]

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.bucket
      ALERTS_TOPIC_ARN = aws_sns_topic.alerts.arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.dcl_contracts_diff]
}
