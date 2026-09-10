resource "aws_lambda_permission" "allow_eventbridge" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.collector.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
  statement_id  = "AllowExecutionFromEventBridge"
}

resource "aws_lambda_function_event_invoke_config" "collector" {
  function_name          = aws_lambda_function.collector.function_name
  maximum_retry_attempts = 2
  destination_config {
    on_failure {
      destination = aws_sns_topic.errors.arn
    }
  }
}

resource "aws_lambda_function" "collector" {
  architectures                  = ["x86_64"]
  filename                       = data.archive_file.lambda.output_path
  function_name                  = "binance-futures-collector"
  handler                        = "lambda_function.lambda_handler"
  layers                         = var.lambda_layers
  memory_size                    = 512
  package_type                   = "Zip"
  reserved_concurrent_executions = -1
  role                           = aws_iam_role.lambda_exec.arn
  runtime                        = "python3.14"
  source_code_hash               = data.archive_file.lambda.output_base64sha256
  skip_destroy                   = false
  tags                           = {}
  timeout                        = 600
  environment {
    variables = {
      PERIOD        = var.period
      S3_BUCKET     = var.s3_bucket
      S3_PREFIX     = var.s3_prefix
      SNS_TOPIC_ARN = aws_sns_topic.errors.arn
      SYMBOLS       = join(",", var.symbols)
    }
  }
  ephemeral_storage {
    size = 512
  }
  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda.name
  }
  tracing_config {
    mode = "PassThrough"
  }
}