provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "binance-futures-data-collector"
      ManagedBy = "terraform"
    }
  }
}
