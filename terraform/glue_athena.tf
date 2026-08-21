resource "aws_glue_catalog_database" "bronze" {
  name = "bronze"
  tags = local.base_tags
}

resource "aws_athena_workgroup" "main" {
  name = "decentraland-data-platform"
  tags = local.base_tags

  configuration {
    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB cost safety net
    result_configuration {
      output_location = "s3://${local.bucket_name}/athena-results/"
    }
  }
}
