# dcl-contracts-daily: Extract -> Diff, with failures routed to the alerts
# SNS topic using the custom notify-slack contract. This state machine is the
# seed of the phase-8 orchestration — later phases add branches to it instead
# of creating new schedulers.
locals {
  sfn_dcl_contracts_tags = { component = "orchestration", layer = "ops" }
}

resource "aws_iam_role" "sfn_dcl_contracts" {
  name = "dcl-contracts-daily-sfn-role"
  tags = local.sfn_dcl_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_dcl_contracts" {
  name = "invoke-lambdas-publish-alerts"
  role = aws_iam_role.sfn_dcl_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
          aws_lambda_function.dcl_contracts.arn,
          aws_lambda_function.dcl_contracts_diff.arn,
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

resource "aws_sfn_state_machine" "dcl_contracts_daily" {
  name     = "dcl-contracts-daily"
  role_arn = aws_iam_role.sfn_dcl_contracts.arn
  tags     = local.sfn_dcl_contracts_tags

  definition = jsonencode({
    Comment = "Daily dcl_contracts snapshot extract + day-over-day diff alert"
    StartAt = "Extract"
    States = {
      # Execution input {} means "today"; {"date": "YYYY-MM-DD"} backfills.
      # ResultPath keeps the original input intact, so Diff sees "date" too.
      Extract = {
        Type       = "Task"
        Resource   = aws_lambda_function.dcl_contracts.arn
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
        Next = "Diff"
      }
      Diff = {
        Type       = "Task"
        Resource   = aws_lambda_function.dcl_contracts_diff.arn
        ResultPath = "$.diff"
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
            component         = "dcl_contracts"
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

# Daily trigger at 06:00 UTC. Removed from the extract Lambda in the explicit-
# partitions change; reintroduced here targeting the state machine instead.
resource "aws_cloudwatch_event_rule" "dcl_contracts_daily" {
  name                = "dcl-contracts-daily"
  description         = "Run the dcl-contracts-daily state machine every day at 06:00 UTC"
  schedule_expression = "cron(0 6 * * ? *)"
  tags                = local.sfn_dcl_contracts_tags
}

resource "aws_iam_role" "eventbridge_sfn_dcl_contracts" {
  name = "dcl-contracts-daily-events-role"
  tags = local.sfn_dcl_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_dcl_contracts" {
  name = "start-dcl-contracts-daily"
  role = aws_iam_role.eventbridge_sfn_dcl_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.dcl_contracts_daily.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "dcl_contracts_daily" {
  rule     = aws_cloudwatch_event_rule.dcl_contracts_daily.name
  arn      = aws_sfn_state_machine.dcl_contracts_daily.arn
  role_arn = aws_iam_role.eventbridge_sfn_dcl_contracts.arn
  # Empty input = "today"; the raw EventBridge event would add noise keys.
  input = "{}"
}
