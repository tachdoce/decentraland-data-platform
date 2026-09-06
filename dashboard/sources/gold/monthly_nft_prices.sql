SELECT
    CAST(f.month || '-01' AS DATE) AS month,
    d.contract_name,
    d.chain_id,
    d.erc_type,
    f.sales_count,
    f.nft_quantity,
    f.usd_total_amount,
    f.usd_median_unit_price
FROM read_parquet('s3://decentraland-data-platform-683569194224/gold/fct_monthly_nft_prices/*/*', hive_partitioning = 1) AS f
INNER JOIN read_parquet('s3://decentraland-data-platform-683569194224/gold/dim_nft_contracts/*') AS d
    ON f.sk_contract = d.sk_contract
