# Bronze log tables, one per chain. No partition projection: the Lambda
# registers each dt explicitly, which keeps the "$partitions" metadata
# convention working (projection tables expose no real partitions).
resource "aws_glue_catalog_table" "onchain_logs" {
  for_each = toset(["ethereum_logs", "polygon_logs"])

  database_name = aws_glue_catalog_database.bronze.name
  name          = each.value
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/${each.value}/"
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
      name = "address"
      type = "string"
    }
    columns {
      name = "topics"
      type = "array<string>"
    }
    columns {
      name = "data"
      type = "string"
    }
    columns {
      name = "extracted_at"
      type = "timestamp"
    }
  }
}
