# Architecture

Serverless AWS pipeline extracting Decentraland on-chain data (Ethereum + Polygon)
and MANA prices, processed through a medallion architecture. Target cost: ~$0/month.

## Medallion lake

<svg viewBox="0 0 860 150" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;">
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b"/>
    </marker>
  </defs>
  <g font-family="sans-serif" text-anchor="middle">
    <rect x="10" y="40" width="140" height="70" rx="10" fill="#92400e"/>
    <text x="80" y="70" fill="#fff" font-size="16" font-weight="bold">bronze</text>
    <text x="80" y="92" fill="#fde68a" font-size="11">raw logs + prices</text>
    <rect x="185" y="40" width="140" height="70" rx="10" fill="#475569"/>
    <text x="255" y="70" fill="#fff" font-size="16" font-weight="bold">staging</text>
    <text x="255" y="92" fill="#cbd5e1" font-size="11">ABI-decoded events</text>
    <rect x="360" y="40" width="140" height="70" rx="10" fill="#94a3b8"/>
    <text x="430" y="70" fill="#fff" font-size="16" font-weight="bold">silver</text>
    <text x="430" y="92" fill="#f1f5f9" font-size="11">clean entities</text>
    <rect x="535" y="40" width="140" height="70" rx="10" fill="#ca8a04"/>
    <text x="605" y="70" fill="#fff" font-size="16" font-weight="bold">gold</text>
    <text x="605" y="92" fill="#fef9c3" font-size="11">facts + dims (KPIs)</text>
    <rect x="710" y="40" width="140" height="70" rx="10" fill="#0f766e"/>
    <text x="780" y="70" fill="#fff" font-size="16" font-weight="bold">this site</text>
    <text x="780" y="92" fill="#99f6e4" font-size="11">static extracts</text>
    <line x1="150" y1="75" x2="183" y2="75" stroke="#64748b" stroke-width="2" marker-end="url(#arrow)"/>
    <line x1="325" y1="75" x2="358" y2="75" stroke="#64748b" stroke-width="2" marker-end="url(#arrow)"/>
    <line x1="500" y1="75" x2="533" y2="75" stroke="#64748b" stroke-width="2" marker-end="url(#arrow)"/>
    <line x1="675" y1="75" x2="708" y2="75" stroke="#64748b" stroke-width="2" marker-end="url(#arrow)"/>
    <text x="167" y="65" fill="#64748b" font-size="10" text-anchor="middle">eth_abi</text>
    <text x="342" y="65" fill="#64748b" font-size="10" text-anchor="middle">dbt</text>
    <text x="517" y="65" fill="#64748b" font-size="10" text-anchor="middle">dbt</text>
    <text x="692" y="65" fill="#64748b" font-size="10" text-anchor="middle">build</text>
  </g>
</svg>

- **bronze**: raw contract logs from BigQuery public blockchain datasets + CoinGecko prices, partitioned by `chain_id` / `dt`
- **staging**: record-level ABI decoding (Python Lambda + eth_abi)
- **silver / gold**: set-based transforms and tests with dbt-athena
- **this site**: aggregated extracts embedded at build time — the bucket stays private

## Daily orchestration (Step Functions)

Every day at 06:00 UTC, EventBridge triggers a state machine:
parallel extractions (Ethereum, Polygon, prices) → decode → `dbt build`
(tests gate publishing) → site rebuild. Failures alert to Slack via SNS.
