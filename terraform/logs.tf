resource "aws_cloudwatch_log_group" "lambda" {
  deletion_protection_enabled = false
  log_group_class             = "STANDARD"
  name                        = "/aws/lambda/binance-futures-collector"
  retention_in_days           = 30
  skip_destroy                = false
  tags                        = {}
}

resource "aws_cloudwatch_metric_alarm" "collector_not_running" {
  alarm_name        = "binance-futures-collector-not-running"
  alarm_description = "No Lambda invocation in 24h — the daily schedule is not firing."

  namespace   = "AWS/Lambda"
  metric_name = "Invocations"
  dimensions = {
    FunctionName = aws_lambda_function.collector.function_name
  }

  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  comparison_operator = "LessThanThreshold"
  threshold           = 1

  # Invocations is not published at all when the function never runs,
  # so missing data must count as breaching or the alarm never fires.
  treat_missing_data = "breaching"

  alarm_actions = [aws_sns_topic.lambda_error_notification.arn]
  ok_actions    = [aws_sns_topic.lambda_error_notification.arn]
}