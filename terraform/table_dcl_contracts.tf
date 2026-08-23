resource "aws_glue_catalog_table" "dcl_contracts" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "dcl_contracts"
  table_type    = "EXTERNAL_TABLE"

  # No partition projection here: the Lambda registers each partition
  # explicitly (ALTER TABLE ADD PARTITION) and the catalog lists exactly
  # the partitions that really exist — same pattern as contracts.
  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/dcl_contracts/"
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
      name = "contract_name"
      type = "string"
    }
  }
}
