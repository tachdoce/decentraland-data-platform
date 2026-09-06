# NFT market overview

```sql monthly_market
SELECT
    month,
    SUM(usd_total_amount) AS total_volume_usd,
    SUM(sales_count) AS total_sales
FROM gold.monthly_nft_prices
GROUP BY month
ORDER BY month
```

<LineChart
    data={monthly_market}
    x=month
    y=total_volume_usd
    title="Total NFT volume per month (USD)"
    yFmt=usd
/>

```sql top_collections
SELECT
    contract_name,
    SUM(usd_total_amount) AS total_volume_usd
FROM gold.monthly_nft_prices
GROUP BY contract_name
ORDER BY total_volume_usd DESC
LIMIT 15
```

<BarChart
    data={top_collections}
    x=contract_name
    y=total_volume_usd
    swapXY=true
    title="Top 15 collections by all-time volume (USD)"
    yFmt=usd
/>
