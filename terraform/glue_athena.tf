resource "aws_glue_catalog_database" "bronze" {
  name = "bronze"
  tags = local.base_tags
}

resource "aws_glue_catalog_database" "silver" {
  name = "silver"
  tags = local.base_tags
}

resource "aws_glue_catalog_database" "staging" {
  name = "staging"
  tags = local.base_tags
}

resource "aws_glue_catalog_database" "gold" {
  name = "gold"
  tags = local.base_tags
}

resource "aws_athena_workgroup" "main" {
  name = "decentraland-data-platform"
  tags = local.base_tags

  configuration {
    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB cost safety net

    # Must NOT enforce: when the workgroup forces its output location,
    # dbt-athena omits external_location from CTAS and tables land under
    # athena-results/tables/<uuid>/ instead of the medallion layout
    # (silver/<table>/). The scan cutoff above still applies either way.
    enforce_workgroup_configuration = false

    result_configuration {
      output_location = "s3://${local.bucket_name}/athena-results/"
    }
  }
}
