# contracts-on-push

Event-driven ingestion of the hand-curated contracts registry. Pushing a CSV to
`landing/contracts/` in S3 emits an EventBridge event that starts this state
machine: the CSV is snapshotted into bronze, then dbt rebuilds the contract
dimensions and runs their tests.

<svg viewBox="0 0 860 120" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;">
  <defs>
    <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b"/>
    </marker>
  </defs>
  <g font-family="sans-serif" text-anchor="middle">
    <ellipse cx="90" cy="60" rx="70" ry="26" fill="#e2e8f0"/>
    <text x="90" y="57" fill="#334155" font-size="12">S3 push event</text>
    <text x="90" y="72" fill="#64748b" font-size="10">landing/contracts/</text>
    <rect x="205" y="27" width="220" height="66" rx="8" fill="#166534"/>
    <text x="315" y="54" fill="#fff" font-size="14" font-weight="bold">LoadContracts</text>
    <text x="315" y="75" fill="#bbf7d0" font-size="11">Lambda: CSV → bronze snapshot</text>
    <rect x="470" y="27" width="220" height="66" rx="8" fill="#166534"/>
    <text x="580" y="54" fill="#fff" font-size="14" font-weight="bold">RunDbt</text>
    <text x="580" y="75" fill="#bbf7d0" font-size="11">dbt build (DAG below) + tests</text>
    <ellipse cx="785" cy="60" rx="50" ry="24" fill="#166534"/>
    <text x="785" y="65" fill="#fff" font-size="12">Succeed</text>
    <line x1="160" y1="60" x2="203" y2="60" stroke="#64748b" stroke-width="2" marker-end="url(#arr)"/>
    <line x1="425" y1="60" x2="468" y2="60" stroke="#64748b" stroke-width="2" marker-end="url(#arr)"/>
    <line x1="690" y1="60" x2="733" y2="60" stroke="#64748b" stroke-width="2" marker-end="url(#arr)"/>
  </g>
</svg>

## dbt execution sequence inside RunDbt

`dbt build` resolves the dependency graph from `ref()` and runs models in
dependency order: silver first, then the gold models that depend on them.
The two chains are independent, so dbt runs them in parallel.

<svg viewBox="0 0 860 230" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;">
  <defs>
    <marker id="arr2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b"/>
    </marker>
  </defs>
  <g font-family="sans-serif" text-anchor="middle">
    <text x="120" y="30" fill="#92400e" font-size="12" font-weight="bold">bronze (source)</text>
    <text x="430" y="30" fill="#64748b" font-size="12" font-weight="bold">silver — step 1</text>
    <text x="740" y="30" fill="#ca8a04" font-size="12" font-weight="bold">gold — step 2</text>
    <rect x="30" y="50" width="180" height="56" rx="8" fill="#92400e"/>
    <text x="120" y="74" fill="#fff" font-size="13" font-weight="bold">contracts</text>
    <text x="120" y="93" fill="#fde68a" font-size="10">curated CSV snapshot</text>
    <rect x="340" y="50" width="180" height="56" rx="8" fill="#64748b"/>
    <text x="430" y="74" fill="#fff" font-size="13" font-weight="bold">dim_contracts</text>
    <text x="430" y="93" fill="#e2e8f0" font-size="10">dedup + lowercase keys</text>
    <rect x="650" y="50" width="180" height="56" rx="8" fill="#ca8a04"/>
    <text x="740" y="74" fill="#fff" font-size="13" font-weight="bold">dim_nft_contracts</text>
    <text x="740" y="93" fill="#fef9c3" font-size="10">NFT subset + sk_contract</text>
    <rect x="30" y="150" width="180" height="56" rx="8" fill="#92400e"/>
    <text x="120" y="174" fill="#fff" font-size="13" font-weight="bold">erc20_tokens</text>
    <text x="120" y="193" fill="#fde68a" font-size="10">curated CSV snapshot</text>
    <rect x="340" y="150" width="180" height="56" rx="8" fill="#64748b"/>
    <text x="430" y="174" fill="#fff" font-size="13" font-weight="bold">dim_erc20_tokens</text>
    <text x="430" y="193" fill="#e2e8f0" font-size="10">dedup + lowercase keys</text>
    <rect x="650" y="150" width="180" height="56" rx="8" fill="#ca8a04"/>
    <text x="740" y="174" fill="#fff" font-size="13" font-weight="bold">dim_currency</text>
    <text x="740" y="193" fill="#fef9c3" font-size="10">currencies by symbol</text>
    <line x1="210" y1="78" x2="338" y2="78" stroke="#64748b" stroke-width="2" marker-end="url(#arr2)"/>
    <line x1="520" y1="78" x2="648" y2="78" stroke="#64748b" stroke-width="2" marker-end="url(#arr2)"/>
    <line x1="210" y1="178" x2="338" y2="178" stroke="#64748b" stroke-width="2" marker-end="url(#arr2)"/>
    <line x1="520" y1="178" x2="648" y2="178" stroke="#64748b" stroke-width="2" marker-end="url(#arr2)"/>
    <text x="274" y="68" fill="#64748b" font-size="10">source()</text>
    <text x="584" y="68" fill="#64748b" font-size="10">ref()</text>
    <text x="274" y="168" fill="#64748b" font-size="10">source()</text>
    <text x="584" y="168" fill="#64748b" font-size="10">ref()</text>
  </g>
</svg>

Each model's schema tests run right after it builds (`dbt build`), so a bad
CSV fails fast and never reaches the published dimensions.
