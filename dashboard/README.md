# Dashboard — Evidence.dev

Static dashboard for the Decentraland Data Platform, built with
[Evidence](https://docs.evidence.dev) (classic, v40 — pinned via the lockfile;
newer "Evidence Studio" versions use a different CLI and syntax).

Source queries under `sources/gold/` read the `gold/` layer parquet files
directly from S3 at build time and embed the aggregated results into the site.
Pages under `pages/` query those embedded extracts in the browser with
DuckDB-WASM — visitors never touch S3 and need no credentials.

## Local development

Requires Node 20+ and AWS credentials with read access to the lake bucket
(any method the default credential chain resolves: profile, env vars, SSO).

```bash
npm install
npm run s3-secret   # once per machine: stores a DuckDB S3 secret (credential chain) in ~/.duckdb
npm run sources     # runs sources/gold/*.sql against S3, materializes extracts
npm run dev         # dev server at localhost:3000
```

## Production build

```bash
npm run sources && npm run build   # static site in ./build
```

## Deployment

`.github/workflows/deploy-dashboard.yml` builds the site and publishes it to
GitHub Pages. It runs **only** on manual dispatch (Actions → deploy-dashboard
→ Run workflow) or when a merge to main touches `dashboard/` — no schedule, so
published data stays as of the last run (the home page shows build time and
data coverage). AWS access uses the OIDC role from `terraform/github_oidc.tf`;
no stored credentials.

## Structure

| Path | Role |
|---|---|
| `sources/gold/` | Build-time SQL against `s3://.../gold/` (DuckDB). Each query becomes an embedded parquet extract — keep them aggregated |
| `pages/` | One Markdown file per page: SQL fences + chart components, rendered client-side |
| `pages/architecture/` | Architecture section: medallion overview and one subpage per Step Functions state machine |
| `evidence.config.yaml` | Theme + plugins (DuckDB datasource only) |

Known limitation: the bucket name is hardcoded in the source SQL (Evidence
classic does not interpolate environment variables in source queries).
