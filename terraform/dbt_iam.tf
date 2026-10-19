locals {
  account_id    = data.aws_caller_identity.current.account_id
  glue_prefix   = "arn:aws:glue:${var.aws_region}:${local.account_id}"
  bucket_arn    = "arn:aws:s3:::${var.s3_bucket}"
  workgroup_arn = "arn:aws:athena:${var.aws_region}:${local.account_id}:workgroup/primary"
}


resource "aws_iam_user" "binance_futures_dbt" {
  name = "binance-futures-dbt"
  tags = {}
}

resource "aws_iam_user_policy" "dbt_access" {
  name = "binance-futures-dbt-access"
  user = aws_iam_user.binance_futures_dbt.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunAthenaQueries"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = local.workgroup_arn
      },
      {
        Sid    = "ReadGlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "${local.glue_prefix}:catalog",
          "${local.glue_prefix}:database/binance_futures",
          "${local.glue_prefix}:table/binance_futures/*",
          "${local.glue_prefix}:database/binance_futures_dbt",
          "${local.glue_prefix}:table/binance_futures_dbt/*",
        ]
      },
      {
        Sid    = "ManageDbtTables"
        Effect = "Allow"
        Action = [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetTableVersions",
        ]
        Resource = [
          "${local.glue_prefix}:catalog",
          "${local.glue_prefix}:database/binance_futures_dbt",
          "${local.glue_prefix}:table/binance_futures_dbt/*",
        ]
      },
      {
        Sid      = "ReadRawData"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${local.bucket_arn}/${var.s3_prefix}/*"
      },
      {
        Sid    = "WriteDbtAndQueryResults"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${local.bucket_arn}/athena_result/*",
          "${local.bucket_arn}/dbt/*",
        ]
      },
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.bucket_arn
      },
    ]
  })
}