# token-prices: manual extraction of DefiLlama daily prices into bronze,
# then dbt build of silver.token_prices. No EventBridge trigger (user
# decision: no cron; phase 8 orchestrates). Failures use the custom
# notify-slack contract.
locals {
  sfn_token_prices_tags = { component = "orchestration", layer = "ops" }
}

resource "aws_iam_role" "sfn_token_prices" {
  name = "token-prices-sfn-role"
  tags = local.sfn_token_prices_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_token_prices" {
  name = "invoke-lambda-publish-alerts"
  role = aws_iam_role.sfn_token_prices.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
          aws_lambda_function.token_prices.arn,
          aws_lambda_function.run_dbt.arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "token_prices" {
  name     = "token-prices"
  role_arn = aws_iam_role.sfn_token_prices.arn
  tags     = local.sfn_token_prices_tags

  definition = jsonencode({
    Comment = "Manual DefiLlama price extraction -> bronze.token_prices"
    StartAt = "ExtractTokenPrices"
    States = {
      # Execution input passes through untouched, so a manual run can
      # carry {} (today) or {start_date, end_date} (backfill).
      ExtractTokenPrices = {
        Type       = "Task"
        Resource   = aws_lambda_function.token_prices.arn
        ResultPath = "$.extract"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "RunDbt"
      }
      RunDbt = {
        Type     = "Task"
        Resource = aws_lambda_function.run_dbt.arn
        Parameters = {
          select = "source:bronze.token_prices+"
        }
        ResultPath = "$.dbt"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        End = true
      }
      # Custom notify-slack contract; Step Functions serializes Message to JSON.
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.alerts.arn
          Message = {
            source            = "step-functions"
            component         = "token-prices"
            status            = "FAILED"
            "detail.$"        = "States.Format('{}: {}', $.error.Error, $.error.Cause)"
            "execution_url.$" = "States.Format('https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/{}', $$.Execution.Id)"
          }
        }
        Next = "FailExecution"
      }
      FailExecution = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "A step failed; details were sent to the alerts topic"
      }
    }
  })
}
