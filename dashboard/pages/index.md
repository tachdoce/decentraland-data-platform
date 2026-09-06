# NFT collection comparison

Monthly NFT prices from `gold.fct_monthly_nft_prices`, joined with `gold.dim_nft_contracts`.

```sql freshness
SELECT MAX(month) AS last_month FROM gold.monthly_nft_prices
```

<LastRefreshed prefix="Site built"/> — data through <Value data={freshness} column=last_month fmt="mmm yyyy"/>.

```sql collections
SELECT
    contract_name,
    SUM(sales_count) AS total_sales
FROM gold.monthly_nft_prices
GROUP BY contract_name
ORDER BY total_sales DESC
```

<Dropdown data={collections} name=col_a value=contract_name title="Collection A" defaultValue="LANDProxy"/>
<Dropdown data={collections} name=col_b value=contract_name title="Collection B" defaultValue="Otherdeed"/>

```sql comparison
SELECT
    month,
    contract_name,
    sales_count,
    nft_quantity,
    usd_total_amount,
    usd_median_unit_price
FROM gold.monthly_nft_prices
WHERE contract_name IN ('${inputs.col_a.value}', '${inputs.col_b.value}')
ORDER BY month, contract_name
```

<LineChart
    data={comparison}
    x=month
    y=usd_median_unit_price
    series=contract_name
    title="Median unit price (USD)"
    yFmt=usd
/>

<BarChart
    data={comparison}
    x=month
    y=sales_count
    series=contract_name
    type=grouped
    title="Sales count per month"
/>

<LineChart
    data={comparison}
    x=month
    y=usd_total_amount
    series=contract_name
    title="Monthly volume (USD)"
    yFmt=usd
/>

## Underlying data

<DataTable data={comparison} search=true/>
