resource "aws_glue_catalog_table" "nft_contracts" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "nft_contracts"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"            = "parquet"
    "projection.enabled"        = "true"
    "projection.dt.type"        = "date"
    "projection.dt.format"      = "yyyy-MM-dd"
    "projection.dt.range"       = "2026-08-01,NOW"
    "storage.location.template" = "s3://${local.bucket_name}/bronze/nft_contracts/dt=$${dt}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/bronze/nft_contracts/"
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
    columns {
      name = "first_mint_dt"
      type = "date"
    }
    columns {
      name = "extract_from_dt"
      type = "date"
    }
  }
}
