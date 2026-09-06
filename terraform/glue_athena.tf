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
    # 20 GB cost safety net (~$0.10 max per query at $5/TB). Raised from
    # 1 GB for fct_monthly_wallet_snapshot: a cumulative snapshot that
    # scans the full fct_nft_transfers/fct_mana_transfers history (with
    # multiple references) every monthly run.
    bytes_scanned_cutoff_per_query = 21474836480

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
