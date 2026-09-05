# contracts-on-push: CSV lands in landing/contracts/ -> ingest -> dbt build.
# Event-driven only (no schedule); failures use the custom notify-slack
# contract, same skeleton as dcl-contracts-daily.
locals {
  sfn_contracts_tags = { component = "orchestration", layer = "ops" }
}

resource "aws_iam_role" "sfn_contracts" {
  name = "contracts-on-push-sfn-role"
  tags = local.sfn_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_contracts" {
  name = "invoke-lambdas-publish-alerts"
  role = aws_iam_role.sfn_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
          aws_lambda_function.contracts.arn,
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

resource "aws_sfn_state_machine" "contracts_on_push" {
  name     = "contracts-on-push"
  role_arn = aws_iam_role.sfn_contracts.arn
  tags     = local.sfn_contracts_tags

  definition = jsonencode({
    Comment = "landing/contracts CSV push -> load-contracts -> dbt build"
    StartAt = "LoadContracts"
    States = {
      # Input is the raw EventBridge S3 event; Parameters rebuilds the S3
      # Records shape the Lambda already understands (no handler change).
      LoadContracts = {
        Type     = "Task"
        Resource = aws_lambda_function.contracts.arn
        Parameters = {
          Records = [{
            s3 = {
              bucket = { "name.$" = "$.detail.bucket.name" }
              object = { "key.$" = "$.detail.object.key" }
            }
          }]
        }
        ResultPath = "$.load"
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
        Type       = "Task"
        Resource   = aws_lambda_function.run_dbt.arn
        # Dimensions only (models + their tests). Facts like silver.sales
        # are deliberately excluded: a CSV push refreshes dimensions, not
        # the heavy incremental models.
        Parameters = {
          select = "dim_contracts dim_erc20_tokens dim_nft_contracts dim_currency"
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
            component         = "contracts"
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

resource "aws_cloudwatch_event_rule" "contracts_on_push" {
  name        = "contracts-on-push"
  description = "Start contracts-on-push when a CSV lands in landing/contracts/"
  tags        = local.sfn_contracts_tags

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.lake.id] }
      object = { key = [{ wildcard = "landing/contracts/*.csv" }] }
    }
  })
}

resource "aws_iam_role" "eventbridge_sfn_contracts" {
  name = "contracts-on-push-events-role"
  tags = local.sfn_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_contracts" {
  name = "start-contracts-on-push"
  role = aws_iam_role.eventbridge_sfn_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.contracts_on_push.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "contracts_on_push" {
  rule     = aws_cloudwatch_event_rule.contracts_on_push.name
  arn      = aws_sfn_state_machine.contracts_on_push.arn
  role_arn = aws_iam_role.eventbridge_sfn_contracts.arn
  # Full event passes through: the state machine reads $.detail.bucket/object.
}
