resource "aws_iam_role_policy_attachment" "sns_destination" {
  policy_arn = aws_iam_policy.sns_destination.arn
  role       = aws_iam_role.lambda_exec.name
}


resource "aws_iam_role_policy_attachment" "basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_exec.name
}

resource "aws_iam_role_policy" "s3_access" {
  name = "binance-futures-collector-s3"
  policy = jsonencode({
    Statement = [{
      Action   = ["s3:GetObject", "s3:PutObject"]
      Effect   = "Allow"
      Resource = "arn:aws:s3:::${var.s3_bucket}/${var.s3_prefix}/*"
      }, {
      Action   = "s3:ListBucket"
      Effect   = "Allow"
      Resource = "arn:aws:s3:::${var.s3_bucket}"
    }]
    Version = "2012-10-17"
  })
  role = aws_iam_role.lambda_exec.name
}

resource "aws_iam_policy" "sns_destination" {
  name        = "binance-futures-collector-sns-publish"
  description = "Allows the collector Lambda to publish failure notifications to the error topic"
  policy = jsonencode({
    Statement = [{
      Action   = "sns:Publish"
      Effect   = "Allow"
      Resource = aws_sns_topic.lambda_error_notification.arn
    }]
    Version = "2012-10-17"
  })
  tags = {}
}

resource "aws_iam_role" "lambda_exec" {
  assume_role_policy = jsonencode({
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
    Version = "2012-10-17"
  })
  description           = "Execution role for the Binance futures collector Lambda."
  force_detach_policies = false
  max_session_duration  = 3600
  name                  = "binance-futures-collector-role"
  path                  = "/"
  tags                  = {}
}