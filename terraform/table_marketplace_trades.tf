# Staging table for decoded V3/V4 marketplace Traded events. No
# partition projection: the Lambda registers each dt explicitly, which
# keeps the "$partitions" metadata convention working.
resource "aws_glue_catalog_table" "marketplace_trades" {
  database_name = aws_glue_catalog_database.staging.name
  name          = "ethereum_marketplace_trades"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/staging/ethereum_marketplace_trades/"
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
      name = "marketplace_address"
      type = "string"
    }
    columns {
      name = "trade_id"
      type = "string"
    }
    columns {
      name = "caller"
      type = "string"
    }
    columns {
      name = "signer"
      type = "string"
    }
    columns {
      name = "order_side"
      type = "string"
    }
    columns {
      name = "buyer"
      type = "string"
    }
    columns {
      name = "seller"
      type = "string"
    }
    columns {
      name = "nft_contract_addresses"
      type = "array<string>"
    }
    columns {
      name = "nft_token_ids"
      type = "array<string>"
    }
    columns {
      name = "nft_asset_types"
      type = "array<bigint>"
    }
    columns {
      name = "nft_beneficiaries"
      type = "array<string>"
    }
    columns {
      name = "payment_currencies"
      type = "array<string>"
    }
    columns {
      name = "payment_amounts"
      type = "array<decimal(38,0)>"
    }
    columns {
      name = "payment_asset_types"
      type = "array<bigint>"
    }
    columns {
      name = "payment_beneficiaries"
      type = "array<string>"
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
