resource "aws_cloudwatch_log_group" "lambda" {
  deletion_protection_enabled = false
  log_group_class             = "STANDARD"
  name                        = "/aws/lambda/binance-futures-collector"
  retention_in_days           = 30
  skip_destroy                = false
  tags                        = {}
}