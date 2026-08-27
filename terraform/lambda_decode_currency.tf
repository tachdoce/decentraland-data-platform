locals {
  decode_currency_tags = { component = "decode", layer = "staging" }
}

# source blocks (not source_dir) so the zip keeps the decode/ package
# directory and the handler resolves as decode.ethereum_erc20_handler.handler.
# Only this Lambda's modules ship.
data "archive_file" "decode_currency" {
  type        = "zip"
  output_path = "${path.module}/build/decode_currency.zip"

  source {
    content  = file("${path.module}/../decode/__init__.py")
    filename = "decode/__init__.py"
  }
  source {
    content  = file("${path.module}/../decode/common.py")
    filename = "decode/common.py"
  }
  source {
    content  = file("${path.module}/../decode/ethereum_erc20_query.py")
    filename = "decode/ethereum_erc20_query.py"
  }
  source {
    content  = file("${path.module}/../decode/ethereum_erc20_handler.py")
    filename = "decode/ethereum_erc20_handler.py"
  }
}

resource "aws_iam_role" "decode_currency" {
  name = "decode-ethereum-currency-transfers-role"
  tags = local.decode_currency_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_currency" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_currency.id
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
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/staging/*",
        ]
      },
      {
        # overwrite_partitions deletes and re-registers dt partitions
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/staging/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        # read bronze data for the extraction query
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/*"
      },
      {
        # wrangler UNLOAD scratch + staging output (overwrite needs delete)
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.lake.arn}/athena-results/*",
          "${aws_s3_bucket.lake.arn}/staging/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "decode_currency_logs" {
  role       = aws_iam_role.decode_currency.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_currency" {
  name              = "/aws/lambda/decode-ethereum-currency-transfers"
  retention_in_days = 7
  tags              = local.decode_currency_tags
}

resource "aws_lambda_function" "decode_currency" {
  function_name    = "decode-ethereum-currency-transfers"
  role             = aws_iam_role.decode_currency.arn
  filename         = data.archive_file.decode_currency.output_path
  source_code_hash = data.archive_file.decode_currency.output_base64sha256
  handler          = "decode.ethereum_erc20_handler.handler"
  runtime          = "python3.13"
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = [local.awssdkpandas_layer_arn]
  tags             = local.decode_currency_tags

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
}
