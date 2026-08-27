# Staging table for decoded Wyvern OrdersMatched sales. No partition
# projection: the Lambda registers each dt explicitly, which keeps the
# "$partitions" metadata convention working.
resource "aws_glue_catalog_table" "wyvern_sales" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_wyvern_sales"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_wyvern_sales/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "transaction_hash"
      type = "string"
    }
    columns {
      name = "log_index"
      type = "bigint"
    }
    columns {
      name = "block_timestamp"
      type = "timestamp"
    }
    columns {
      name = "wyvern_address"
      type = "string"
    }
    columns {
      name = "buy_hash"
      type = "string"
    }
    columns {
      name = "sell_hash"
      type = "string"
    }
    columns {
      name = "maker"
      type = "string"
    }
    columns {
      name = "taker"
      type = "string"
    }
    columns {
      name = "price"
      type = "decimal(38,0)"
    }
    columns {
      name = "metadata"
      type = "string"
    }
    columns {
      name = "bronze_extracted_at"
      type = "timestamp"
    }
    columns {
      name = "decoded_at"
      type = "timestamp"
    }
  }
}
