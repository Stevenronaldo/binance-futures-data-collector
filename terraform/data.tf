data "archive_file" "lambda" {
  type        = "zip"
  output_path = "${path.module}/build/lambda_function.zip"

  source {
    content  = file("${path.module}/../src/lambda_function.py")
    filename = "lambda_function.py"
  }

  source {
    content  = file("${path.module}/../src/utils.py")
    filename = "utils.py"
  }
}

data "aws_caller_identity" "current" {}