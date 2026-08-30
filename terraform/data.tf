data "archive_file" "lambda" {
  type        = "zip"
  output_path = "${path.module}/build/lambda_function.zip"

  source {
    content  = file("${path.module}/../src/binance-futures-data-collector.py")
    filename = "lambda_function.py"
  }
}

data "aws_caller_identity" "current" {}