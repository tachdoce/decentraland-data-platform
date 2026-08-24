locals {
  alerts_tags       = { component = "alerts", layer = "ops" }
  webhook_ssm_param = "/decentraland/slack_webhook_url"
  monitored_lambdas = [
    aws_lambda_function.dcl_contracts.function_name,
    aws_lambda_function.dcl_contracts_diff.function_name,
    aws_lambda_function.contracts.function_name,
    aws_lambda_function.run_dbt.function_name,
    aws_lambda_function.onchain_logs.function_name,
  ]
}

# Central alerts hub: CloudWatch Alarms publish here today; Step Functions
# and the dbt task will publish to the same topic in later phases.
resource "aws_sns_topic" "alerts" {
  name = "decentraland-alerts"
  tags = local.alerts_tags
}

# One alarm per pipeline Lambda on its Errors metric. Failure-only:
# alarm_actions fires on ALARM, and there are deliberately no ok_actions.
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = toset(local.monitored_lambdas)

  alarm_name          = "lambda-errors-${each.value}"
  alarm_description   = "Errors >= 1 in ${each.value}"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = each.value }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  # No data (Lambda not invoked) is normal, not a failure; it also lets the
  # alarm return to OK so the next day's failure re-triggers a notification.
  treat_missing_data = "notBreaching"
  alarm_actions      = [aws_sns_topic.alerts.arn]
  tags               = local.alerts_tags
}

data "archive_file" "notify_slack_zip" {
  type        = "zip"
  output_path = "${path.module}/build/notify_slack.zip"

  source {
    content  = file("${path.module}/../alerts/notify_slack/handler.py")
    filename = "alerts/notify_slack/handler.py"
  }
  source {
    content  = ""
    filename = "alerts/__init__.py"
  }
  source {
    content  = ""
    filename = "alerts/notify_slack/__init__.py"
  }
}

resource "aws_iam_role" "notify_slack" {
  name = "notify-slack-role"
  tags = local.alerts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# The webhook URL is a secret created outside Terraform (aws ssm put-parameter,
# SecureString) so it never lands in the repo or the tfstate; the role may read
# only that one parameter. Decryption uses the default aws/ssm KMS key, which
# allows account principals via SSM without an explicit kms:Decrypt.
resource "aws_iam_role_policy" "notify_slack_ssm" {
  name = "read-slack-webhook-param"
  role = aws_iam_role.notify_slack.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "ssm:GetParameter"
      Resource = "arn:aws:ssm:us-east-1:${data.aws_caller_identity.current.account_id}:parameter${local.webhook_ssm_param}"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "notify_slack_logs" {
  role       = aws_iam_role.notify_slack.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "notify_slack" {
  name              = "/aws/lambda/notify-slack"
  retention_in_days = 7
  tags              = local.alerts_tags
}

resource "aws_lambda_function" "notify_slack" {
  function_name = "notify-slack"
  role          = aws_iam_role.notify_slack.arn
  tags          = local.alerts_tags

  filename         = data.archive_file.notify_slack_zip.output_path
  source_code_hash = data.archive_file.notify_slack_zip.output_base64sha256

  handler     = "alerts.notify_slack.handler.handler"
  runtime     = "python3.13"
  timeout     = 30
  memory_size = 128

  environment {
    variables = {
      WEBHOOK_SSM_PARAM = local.webhook_ssm_param
    }
  }

  depends_on = [aws_cloudwatch_log_group.notify_slack]
}

resource "aws_lambda_permission" "notify_slack_sns" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notify_slack.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.alerts.arn
}

resource "aws_sns_topic_subscription" "alerts_to_slack" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.notify_slack.arn

  depends_on = [aws_lambda_permission.notify_slack_sns]
}
