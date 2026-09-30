variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-southeast-1"
}

variable "s3_bucket" {
  description = "Bucket holding the Parquet datasets and watermarks"
  type        = string
}

variable "s3_prefix" {
  description = "S3 bucket prefix"
  type        = string
  default     = "binance-futures"
}

variable "sns_email" {
  description = "email to receive sns notification"
  type        = string
}

variable "symbols" {
  description = "Futures symbols to collect"
  type        = list(string)
  default = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "HYPEUSDT", "XRPUSDT",
    "TRXUSDT", "ZECUSDT", "DOGEUSDT", "XMRUSDT", "LINKUSDT", "ADAUSDT",
    "XLMUSDT", "UNIUSDT", "BCHUSDT", "NEARUSDT", "LTCUSDT", "AVAXUSDT",
    "SUIUSDT", "TAOUSDT", "AAVEUSDT", "ASTERUSDT", "PUMPUSDT", "ONDOUSDT",
    "DOTUSDT", "ENAUSDT", "1000PEPEUSDT", "WLDUSDT", "ARBUSDT", "POLUSDT"
  ]
}

variable "period" {
  description = "Candle/aggregation period"
  type        = string
  default     = "1h"

  validation {
    condition     = contains(["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"], var.period)
    error_message = "period must be one that all collected endpoints accept."
  }
}

variable "lambda_layers" {
  description = "Layer ARNs providing requests and pandas/pyarrow"
  type        = list(string)
  default = [
    "arn:aws:lambda:ap-southeast-1:770693421928:layer:Klayers-p314-requests:3",
    "arn:aws:lambda:ap-southeast-1:336392948345:layer:AWSSDKPandas-Python314:7",
  ]
}


