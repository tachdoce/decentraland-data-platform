# Staging table for decoded Marketplace V2 order events. No partition
# projection: the Lambda registers each dt explicitly, which keeps the
# "$partitions" metadata convention working.
resource "aws_glue_catalog_table" "marketplace_v2_orders" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_marketplace_v2_orders"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_marketplace_v2_orders/"
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
      name = "event_name"
      type = "string"
    }
    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "asset_id"
      type = "string"
    }
    columns {
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "seller"
      type = "string"
    }
    columns {
      name = "buyer"
      type = "string"
    }
    columns {
      name = "total_price"
      type = "decimal(38,0)"
    }
    columns {
      name = "expires_at"
      type = "timestamp"
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
