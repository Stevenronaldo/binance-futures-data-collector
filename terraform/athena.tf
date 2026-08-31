resource "aws_glue_catalog_database" "binance_futures" {
  name = "binance_futures"
}

resource "aws_glue_catalog_table" "funding_rate" {
  name          = "funding_rate"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=fundingRate"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "fundingtime"
      type = "bigint"
    }
    columns {
      name = "fundingrate"
      type = "double"
    }
    columns {
      name = "markprice"
      type = "double"
    }
    columns {
      name = "ratetype"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "global_ls_account_ratio" {
  name          = "global_ls_account_ratio"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=globalLongShortAccountRatio"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "longaccount"
      type = "double"
    }
    columns {
      name = "longshortratio"
      type = "double"
    }
    columns {
      name = "shortaccount"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "index_price_klines" {
  name          = "index_price_klines"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=indexPriceKlines"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "open_time"
      type = "bigint"
    }
    columns {
      name = "open"
      type = "double"
    }
    columns {
      name = "high"
      type = "double"
    }
    columns {
      name = "low"
      type = "double"
    }
    columns {
      name = "close"
      type = "double"
    }
    columns {
      name = "close_time"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "klines" {
  name          = "klines"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=klines"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "open_time"
      type = "bigint"
    }
    columns {
      name = "open"
      type = "double"
    }
    columns {
      name = "high"
      type = "double"
    }
    columns {
      name = "low"
      type = "double"
    }
    columns {
      name = "close"
      type = "double"
    }
    columns {
      name = "volume"
      type = "double"
    }
    columns {
      name = "close_time"
      type = "bigint"
    }
    columns {
      name = "quote_volume"
      type = "double"
    }
    columns {
      name = "num_trades"
      type = "bigint"
    }
    columns {
      name = "taker_buy_base"
      type = "double"
    }
    columns {
      name = "taker_buy_quote"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "open_interest" {
  name          = "open_interest"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=openInterestHist"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "sumopeninterest"
      type = "double"
    }
    columns {
      name = "sumopeninterestvalue"
      type = "double"
    }
    columns {
      name = "cmccirculatingsupply"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "top_ls_account_ratio" {
  name          = "top_ls_account_ratio"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=topLongShortAccountRatio"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "longaccount"
      type = "double"
    }
    columns {
      name = "longshortratio"
      type = "double"
    }
    columns {
      name = "shortaccount"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "top_ls_position_ratio" {
  name          = "top_ls_position_ratio"
  database_name = aws_glue_catalog_database.binance_futures.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "hadoop"

  partition_keys {
    name = "symbol"
    type = "string"
  }

  parameters = {
    EXTERNAL                   = "TRUE"
    "projection.enabled"       = "true"
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", var.symbols)
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/binance-futures/endpoint=topLongShortPositionRatio"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "longaccount"
      type = "double"
    }
    columns {
      name = "longshortratio"
      type = "double"
    }
    columns {
      name = "shortaccount"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "bigint"
    }
  }
}