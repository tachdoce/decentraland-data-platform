resource "aws_glue_catalog_table" "erc20_tokens" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "erc20_tokens"
  table_type    = "EXTERNAL_TABLE"

  # No partition projection here: snapshots are infrequent, so the Lambda
  # registers each partition explicitly (ALTER TABLE ADD PARTITION) and the
  # catalog lists exactly the partitions that really exist.
  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/erc20_tokens/"
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
      name = "name"
      type = "string"
    }
    columns {
      name = "fsym"
      type = "string"
    }
    columns {
      name = "decimals"
      type = "int"
    }
    columns {
      name = "fetch_price"
      type = "boolean"
    }
  }
}
