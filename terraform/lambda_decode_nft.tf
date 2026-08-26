locals {
  decode_nft_tags = { component = "decode", layer = "staging" }

  # AWS-managed AWS SDK for Pandas layer (pandas + pyarrow + awswrangler):
  # lets this Lambda ship as a plain zip instead of a container image.
  # Version probed via `aws lambda get-layer-version-by-arn`.
  awssdkpandas_layer_arn = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313-Arm64:16"
}

data "archive_file" "decode_nft" {
  type        = "zip"
  source_dir  = "${path.module}/../decode"
  output_path = "${path.module}/build/decode_nft.zip"
  excludes    = ["__pycache__"]
}

resource "aws_iam_role" "decode_nft" {
  name = "decode-nft-transfers-role"
  tags = local.decode_nft_tags
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "decode_nft" {
  name = "athena-glue-s3"
  role = aws_iam_role.decode_nft.id
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
          aws_glue_catalog_database.staging.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
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
        # read bronze + dim_contracts data for the extraction query
        Effect = "Allow"
        Action = "s3:GetObject"
        Resource = [
          "${aws_s3_bucket.lake.arn}/bronze/*",
          "${aws_s3_bucket.lake.arn}/silver/*",
        ]
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

resource "aws_iam_role_policy_attachment" "decode_nft_logs" {
  role       = aws_iam_role.decode_nft.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "decode_nft" {
  name              = "/aws/lambda/decode-nft-transfers"
  retention_in_days = 7
  tags              = local.decode_nft_tags
}

resource "aws_lambda_function" "decode_nft" {
  function_name    = "decode-nft-transfers"
  role             = aws_iam_role.decode_nft.arn
  filename         = data.archive_file.decode_nft.output_path
  source_code_hash = data.archive_file.decode_nft.output_base64sha256
  handler          = "decode.handler.handler"
  runtime          = "python3.13"
  architectures    = ["arm64"]
  timeout          = 300
  memory_size      = 2048
  layers           = [local.awssdkpandas_layer_arn]
  tags             = local.decode_nft_tags

  environment {
    variables = {
      LAKE_BUCKET      = aws_s3_bucket.lake.id
      ATHENA_WORKGROUP = aws_athena_workgroup.main.name
    }
  }
}
