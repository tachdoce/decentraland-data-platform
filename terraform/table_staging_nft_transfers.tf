# Staging table written by the decode-nft-transfers Lambda (awswrangler
# registers the dt partitions on each write). No partition projection, so
# the "$partitions" metadata convention keeps working.
resource "aws_glue_catalog_table" "staging_nft_transfers" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_nft_transfers"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_nft_transfers/"
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
      name = "contract_address"
      type = "string"
    }
    columns {
      name = "erc_type"
      type = "int"
    }
    columns {
      name = "token_id"
      type = "string"
    }
    columns {
      name = "quantity"
      type = "decimal(38,0)"
    }
    columns {
      name = "from_address"
      type = "string"
    }
    columns {
      name = "to_address"
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
