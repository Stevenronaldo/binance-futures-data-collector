resource "aws_sns_topic_subscription" "email" {
  endpoint             = var.sns_email
  protocol             = "email"
  raw_message_delivery = false
  topic_arn            = aws_sns_topic.lambda_error_notification.arn
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_sns_topic" "lambda_error_notification" {
  content_based_deduplication = false
  fifo_topic                  = false
  name                        = "lambda-error-notification"
  policy = jsonencode({
    Id = "__default_policy_ID"
    Statement = [{
      Action = ["SNS:Publish", "SNS:RemovePermission", "SNS:SetTopicAttributes", "SNS:DeleteTopic", "SNS:ListSubscriptionsByTopic", "SNS:GetTopicAttributes", "SNS:AddPermission", "SNS:Subscribe"]
      Condition = {
        StringEquals = {
          "AWS:SourceAccount" = data.aws_caller_identity.current.account_id
        }
      }
      Effect = "Allow"
      Principal = {
        AWS = "*"
      }
      Resource = "arn:aws:sns:${var.aws_region}:${data.aws_caller_identity.current.account_id}:lambda-error-notification"
      Sid      = "__default_statement_ID"
    }]
    Version = "2008-10-17"
  })
  tags           = {}
  tracing_config = "PassThrough"
}