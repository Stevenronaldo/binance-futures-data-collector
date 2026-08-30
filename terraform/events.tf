resource "aws_cloudwatch_event_target" "lambda" {
  arn            = aws_lambda_function.collector.arn
  event_bus_name = "default"
  force_destroy  = false
  rule           = aws_cloudwatch_event_rule.daily.name
  target_id      = "binance-futures-collector"
}

resource "aws_cloudwatch_event_rule" "daily" {
  event_bus_name      = "default"
  force_destroy       = false
  name                = "binance-futures-collector-daily"
  schedule_expression = "cron(0 1 * * ? *)"
  state               = "ENABLED"
  tags                = {}
}