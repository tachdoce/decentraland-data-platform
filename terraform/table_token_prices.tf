resource "aws_glue_catalog_table" "token_prices" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "token_prices"
  table_type    = "EXTERNAL_TABLE"

  # Monthly partitions (prices are tiny: ~22 rows/day; daily partitions
  # would be ~2,800 3-KB files). No partition projection: the Lambda
  # registers each month explicitly.
  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/token_prices/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "chain_id"
      type = "int"
    }
    columns {
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "dt"
      type = "date"
    }
    columns {
      name = "price_usd"
      type = "double"
    }
    columns {
      name = "price_ts"
      type = "timestamp"
    }
    columns {
      name = "confidence"
      type = "double"
    }
    columns {
      name = "extracted_at"
      type = "timestamp"
    }
  }
}
