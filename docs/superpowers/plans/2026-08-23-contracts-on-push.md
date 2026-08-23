# contracts-on-push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pushing `contracts.csv` to `landing/contracts/` triggers ingestion and, on success, a dbt build of `silver.dim_contracts` — fully inside AWS, failures alerting red to Slack.

**Architecture:** A `contracts-on-push` Step Functions state machine (S3 → EventBridge trigger) chains the existing `load-contracts` Lambda with a new `run-dbt` container-image Lambda that runs `dbt build --select source:bronze.contracts+` against Athena. Failures Catch into the existing `decentraland-alerts` SNS → notify-slack contract.

**Tech Stack:** dbt-core 1.12.x + dbt-athena 1.11.x, Python 3.13 Lambda container image (ECR), Step Functions, EventBridge, Terraform.

**Spec:** `docs/superpowers/specs/2026-08-23-contracts-on-push-design.md`

## Global Constraints

- All artifacts in English; chat in Spanish (CLAUDE.md).
- Terraform only; never console. Always read `terraform plan` before apply.
- Bucket name always `decentraland-data-platform-${account_id}` via interpolation.
- Tags: per-resource `component` + `layer` locals, provider default_tags cover the rest.
- Athena runs only in workgroup `decentraland-data-platform`.
- Region us-east-1. Lambda runtime python3.13. Log groups: 7-day retention.
- dbt selection is always `source:bronze.contracts+` unless the event overrides it.
- TDD: failing test → minimal code → pass → commit. Run `.venv/bin/pytest` and `.venv/bin/ruff check .` before every commit.
- Commit locally after each task; NEVER `git push` without the user's explicit OK.

---

### Task 1: `silver` Glue database (Terraform)

The dbt build (local in Task 2, Lambda later) needs the `silver` database to exist; creating it in Terraform means no role ever needs `glue:CreateDatabase`.

**Files:**
- Modify: `terraform/glue_athena.tf` (append below the `bronze` database resource)

**Interfaces:**
- Produces: `aws_glue_catalog_database.silver` (referenced by Task 4 IAM).

- [ ] **Step 1: Add the resource**

```hcl
resource "aws_glue_catalog_database" "silver" {
  name = "silver"
  tags = { component = "lake", layer = "silver" }
}
```

Match the surrounding style of the existing `bronze` resource in that file (copy its tag keys if they differ from the above).

- [ ] **Step 2: Plan and apply**

Run: `cd terraform && terraform plan` — expect exactly `1 to add` (the database). Then `terraform apply`.

- [ ] **Step 3: Commit**

```bash
git add terraform/glue_athena.tf
git commit -m "feat: silver Glue database"
```

---

### Task 2: `silver.dim_contracts` model + dbt tests

**Files:**
- Modify: `dbt/models/silver/dim_contracts.sql` (replace skeleton)
- Modify: `dbt/models/silver/schema.yml` (dim_contracts columns/tests)
- Create: `dbt/tests/assert_dim_contracts_unique_key.sql`
- Create: `dbt/tests/assert_dim_contracts_not_empty.sql`

**Interfaces:**
- Consumes: `source('bronze', 'contracts')` (already declared in `dbt/models/silver/sources.yml`).
- Produces: view `silver.dim_contracts` with columns `chain_id, contract_address, contract_name, dcl_contract, erc_type, first_mint_dt, extract_from_dt, snapshot_dt`. `dim_contracts_changes` keeps compiling: it only selects `chain_id, contract_address` from `ref('dim_contracts')`, both still present.

- [ ] **Step 1: Replace the model**

`dbt/models/silver/dim_contracts.sql`:

```sql
-- Curated contract dimension. Single source of truth: bronze.contracts
-- (the hand-curated CSV). The official registry (bronze.dcl_contracts) is
-- deliberately NOT merged here — its daily diff only alerts a human to
-- update the CSV. Grain: one row per (chain_id, contract_address).

with latest as (

    select max(dt) as dt
    from {{ source('bronze', 'contracts') }}

)

select
    c.chain_id,
    c.contract_address,
    c.contract_name,
    c.dcl_contract,
    c.erc_type,
    c.first_mint_dt,
    c.extract_from_dt,
    c.dt as snapshot_dt
from {{ source('bronze', 'contracts') }} c
inner join latest on c.dt = latest.dt
where c.erc_type != -1
```

- [ ] **Step 2: Update schema.yml tests**

In `dbt/models/silver/schema.yml`, replace the `dim_contracts` entry (keep `dim_contracts_changes` untouched):

```yaml
  - name: dim_contracts
    description: >
      Curated contract dimension from the latest bronze.contracts snapshot
      (the hand-curated CSV, single source of truth). erc_type = -1 rows
      are excluded here; consumers never see them. One row per
      (chain_id, contract_address).
    columns:
      - name: chain_id
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1, 137]
                quote: false
      - name: contract_address
        data_tests:
          - not_null
      - name: dcl_contract
        data_tests:
          - not_null
      - name: erc_type
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: [0, 20, 721, 1155]
                quote: false
```

- [ ] **Step 3: Singular tests**

`dbt/tests/assert_dim_contracts_unique_key.sql`:

```sql
-- Fails when (chain_id, contract_address) is not unique in the dimension.
select
    chain_id,
    contract_address,
    count(*) as n
from {{ ref('dim_contracts') }}
group by chain_id, contract_address
having count(*) > 1
```

`dbt/tests/assert_dim_contracts_not_empty.sql`:

```sql
-- Fails when the dimension is empty: guards against max(dt) over a table
-- with no partitions silently producing zero rows.
select n
from (
    select count(*) as n
    from {{ ref('dim_contracts') }}
)
where n = 0
```

- [ ] **Step 4: Parse, then build locally against Athena**

```bash
cd dbt && ../.venv/bin/dbt parse
../.venv/bin/dbt build --select source:bronze.contracts+
```

Expected: `dim_contracts` view created, `dim_contracts_changes` view created, all tests PASS. Sanity-check the row count (expect **962** = 971 − 9 `erc_type=-1`):

```bash
aws athena start-query-execution --work-group decentraland-data-platform \
  --query-string "select count(*) from silver.dim_contracts"
# then aws athena get-query-results --query-execution-id <id>
```

- [ ] **Step 5: Commit**

```bash
git add dbt/models/silver/ dbt/tests/
git commit -m "feat: silver.dim_contracts from latest curated snapshot, erc_type=-1 excluded"
```

---

### Task 3: `run-dbt` handler (TDD)

**Files:**
- Create: `dbt_runner/__init__.py` (empty)
- Create: `dbt_runner/handler.py`
- Test: `tests/test_dbt_runner.py`

**Interfaces:**
- Consumes: env vars `DBT_PROJECT_DIR`, `ATHENA_WORKGROUP`, `ALERTS_TOPIC_ARN`; `dbt.cli.main.dbtRunner`.
- Produces: `handler(event, context)` — event `{}` or `{"select": "<selector>"}`; returns `{"select": ..., "changes": {<change_type>: count}}`; raises `RuntimeError` on dbt failure (Step Functions catches it). `DEFAULT_SELECT = "source:bronze.contracts+"`. Handler string for Terraform: `dbt_runner.handler.handler`.

- [ ] **Step 1: Write the failing tests**

`tests/test_dbt_runner.py`:

```python
import json
from types import SimpleNamespace

import pytest

import dbt_runner.handler as h


class FakeDbtRunner:
    """Captures invoke args and returns a canned result."""

    calls: list[list[str]] = []
    success = True

    def invoke(self, args):
        FakeDbtRunner.calls.append(list(args))
        return SimpleNamespace(success=FakeDbtRunner.success, exception=None)


class FakeAthena:
    """One changes query returning the configured data rows."""

    def __init__(self, data_rows, state="SUCCEEDED"):
        self.data_rows = data_rows
        self.state = state

    def start_query_execution(self, QueryString, WorkGroup):
        assert WorkGroup == "decentraland-data-platform"
        return {"QueryExecutionId": "qid"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self.state}}}

    def get_query_results(self, QueryExecutionId):
        header = {"Data": [{"VarCharValue": "change_type"}]}
        rows = [{"Data": [{"VarCharValue": v}]} for v in self.data_rows]
        return {"ResultSet": {"Rows": [header] + rows}}


class FakeSNS:
    def __init__(self):
        self.published = []

    def publish(self, TopicArn, Message):
        self.published.append({"topic": TopicArn, "message": json.loads(Message)})


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/var/task/dbt")
    monkeypatch.setenv("ATHENA_WORKGROUP", "decentraland-data-platform")
    monkeypatch.setenv("ALERTS_TOPIC_ARN", "arn:aws:sns:us-east-1:1:decentraland-alerts")


def _wire(monkeypatch, athena, sns):
    FakeDbtRunner.calls = []
    monkeypatch.setattr(h, "dbtRunner", FakeDbtRunner)
    monkeypatch.setattr(
        h.boto3, "client", lambda svc: {"athena": athena, "sns": sns}[svc]
    )


def test_default_selector_and_no_changes(env, monkeypatch):
    sns = FakeSNS()
    _wire(monkeypatch, FakeAthena([]), sns)
    FakeDbtRunner.success = True

    result = h.handler({}, None)

    args = FakeDbtRunner.calls[0]
    assert args[:3] == ["build", "--select", "source:bronze.contracts+"]
    assert "--project-dir" in args and "/var/task/dbt" in args
    assert "--target-path" in args and "/tmp/dbt-target" in args
    assert result == {"select": "source:bronze.contracts+", "changes": {}}
    assert sns.published == []


def test_selector_override(env, monkeypatch):
    _wire(monkeypatch, FakeAthena([]), FakeSNS())
    FakeDbtRunner.success = True

    result = h.handler({"select": "dim_contracts"}, None)

    assert FakeDbtRunner.calls[0][:3] == ["build", "--select", "dim_contracts"]
    assert result["select"] == "dim_contracts"


def test_dbt_failure_raises_before_touching_athena(env, monkeypatch):
    def boom(svc):  # any AWS call after a failed build is a bug
        raise AssertionError("no AWS client expected")

    FakeDbtRunner.calls = []
    monkeypatch.setattr(h, "dbtRunner", FakeDbtRunner)
    monkeypatch.setattr(h.boto3, "client", boom)
    FakeDbtRunner.success = False

    with pytest.raises(RuntimeError, match="dbt build failed"):
        h.handler({}, None)


def test_changes_publish_info(env, monkeypatch):
    sns = FakeSNS()
    _wire(monkeypatch, FakeAthena(["added", "added", "renamed"]), sns)
    FakeDbtRunner.success = True

    result = h.handler({}, None)

    assert result["changes"] == {"added": 2, "renamed": 1}
    assert len(sns.published) == 1
    msg = sns.published[0]["message"]
    assert msg["source"] == "dbt"
    assert msg["component"] == "dbt build"
    assert msg["status"] == "INFO"
    assert "added=2" in msg["detail"] and "renamed=1" in msg["detail"]


def test_changes_query_failure_raises(env, monkeypatch):
    _wire(monkeypatch, FakeAthena([], state="FAILED"), FakeSNS())
    FakeDbtRunner.success = True

    with pytest.raises(RuntimeError, match="changes query"):
        h.handler({}, None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_dbt_runner.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dbt_runner'`.

- [ ] **Step 3: Implement the handler**

`dbt_runner/handler.py`:

```python
"""run-dbt Lambda: builds the dbt models that depend on bronze.contracts.

Invoked by the contracts-on-push state machine after a successful CSV
ingestion. Event: {} for the default selector, {"select": "<selector>"}
for manual runs. Raises on any dbt failure so Step Functions catches it.

After a successful build, queries silver.dim_contracts_changes and posts
an INFO notification to the alerts topic when it has rows (custom
notify-slack contract; the changes model is a skeleton today, so this
never fires until its diff logic lands).
"""

import json
import os
import time

import boto3
from dbt.cli.main import dbtRunner

DEFAULT_SELECT = "source:bronze.contracts+"
CHANGES_QUERY = "select change_type from silver.dim_contracts_changes"


def run_dbt(select: str) -> None:
    project_dir = os.environ["DBT_PROJECT_DIR"]
    # Lambda's filesystem is read-only except /tmp: dbt artifacts and logs
    # must be redirected there.
    result = dbtRunner().invoke(
        [
            "build",
            "--select", select,
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--target-path", "/tmp/dbt-target",
            "--log-path", "/tmp/dbt-logs",
        ]
    )
    if not result.success:
        raise RuntimeError(
            f"dbt build failed: {result.exception or 'model or test failures'}"
        )


def fetch_change_counts(athena) -> dict[str, int]:
    qid = athena.start_query_execution(
        QueryString=CHANGES_QUERY, WorkGroup=os.environ["ATHENA_WORKGROUP"]
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(
            f"changes query {status['State']}: {status.get('StateChangeReason', '')}"
        )
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
    counts: dict[str, int] = {}
    for row in rows:
        change_type = row["Data"][0].get("VarCharValue", "unknown")
        counts[change_type] = counts.get(change_type, 0) + 1
    return counts


def handler(event, context):
    select = (event or {}).get("select") or DEFAULT_SELECT
    run_dbt(select)

    counts = fetch_change_counts(boto3.client("athena"))
    if counts:
        detail = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        boto3.client("sns").publish(
            TopicArn=os.environ["ALERTS_TOPIC_ARN"],
            Message=json.dumps(
                {
                    "source": "dbt",
                    "component": "dbt build",
                    "status": "INFO",
                    "detail": f"dim_contracts changes: {detail}",
                }
            ),
        )
    return {"select": select, "changes": counts}
```

Create empty `dbt_runner/__init__.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_dbt_runner.py -v` → all PASS.
Then the full gate: `.venv/bin/pytest tests/ -q && .venv/bin/ruff check .`

- [ ] **Step 5: Commit**

```bash
git add dbt_runner/ tests/test_dbt_runner.py
git commit -m "feat: run-dbt Lambda handler — dbt build + INFO on contract changes"
```

---

### Task 4: Container image, ECR and the `run-dbt` Lambda (Terraform)

**Files:**
- Create: `dbt_runner/requirements.txt`
- Create: `dbt_runner/Dockerfile`
- Create: `.dockerignore` (repo root)
- Create: `terraform/lambda_run_dbt.tf`
- Modify: `terraform/alerts.tf` (add `run-dbt` to `monitored_lambdas`)

**Interfaces:**
- Consumes: `aws_glue_catalog_database.silver` (Task 1), `dbt_runner.handler.handler` (Task 3), existing `aws_sns_topic.alerts`, `aws_athena_workgroup.main`, `aws_s3_bucket.lake`, `aws_glue_catalog_database.bronze`.
- Produces: `aws_lambda_function.run_dbt` (function name `run-dbt`) for Task 5's state machine.

- [ ] **Step 1: Image definition**

`dbt_runner/requirements.txt` (pin to the locally proven pair):

```
dbt-core==1.12.3
dbt-athena==1.11.0
```

`dbt_runner/Dockerfile` (build context = repo root, so it can copy `dbt/`):

```dockerfile
FROM public.ecr.aws/lambda/python:3.13

COPY dbt_runner/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code only: the dbt project and its secret-free profiles.yml. Reference
# data is never baked into images (repo convention).
COPY dbt/ /var/task/dbt/
COPY dbt_runner/__init__.py dbt_runner/handler.py /var/task/dbt_runner/

CMD ["dbt_runner.handler.handler"]
```

`.dockerignore` (repo root — keeps `dbt/target` artifacts and junk out of the image):

```
dbt/target
dbt/logs
**/__pycache__
.venv
terraform
```

- [ ] **Step 2: Terraform for ECR + Lambda**

`terraform/lambda_run_dbt.tf`:

```hcl
locals {
  run_dbt_tags = { component = "dbt", layer = "silver" }
}

resource "aws_ecr_repository" "run_dbt" {
  name         = "decentraland-run-dbt"
  force_delete = true
  tags         = local.run_dbt_tags
}

# Keep only the 3 most recent images so ECR storage stays near the free tier.
resource "aws_ecr_lifecycle_policy" "run_dbt" {
  repository = aws_ecr_repository.run_dbt.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 3 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}

# Resolves the digest of the pushed :latest tag; a new push + apply
# redeploys the function (image_uri changes with the digest).
data "aws_ecr_image" "run_dbt" {
  repository_name = aws_ecr_repository.run_dbt.name
  image_tag       = "latest"
}

resource "aws_iam_role" "run_dbt" {
  name = "run-dbt-role"
  tags = local.run_dbt_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "run_dbt" {
  name = "dbt-athena-glue-s3-sns"
  role = aws_iam_role.run_dbt.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = aws_athena_workgroup.main.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/bronze/*",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
        ]
      },
      # dbt materializes silver views: create/replace/drop table metadata
      {
        Effect = "Allow"
        Action = ["glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"]
        Resource = [
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/silver/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.lake.arn}/bronze/contracts/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lake.arn}/athena-results/*"
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "run_dbt_logs" {
  role       = aws_iam_role.run_dbt.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "run_dbt" {
  name              = "/aws/lambda/run-dbt"
  retention_in_days = 7
  tags              = local.run_dbt_tags
}

resource "aws_lambda_function" "run_dbt" {
  function_name = "run-dbt"
  role          = aws_iam_role.run_dbt.arn
  tags          = local.run_dbt_tags

  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.run_dbt.repository_url}@${data.aws_ecr_image.run_dbt.image_digest}"
  architectures = ["arm64"] # native build on Apple Silicon, cheaper on Lambda

  timeout     = 300
  memory_size = 1024

  environment {
    variables = {
      DBT_PROJECT_DIR                = "/var/task/dbt"
      ATHENA_WORKGROUP               = aws_athena_workgroup.main.name
      ALERTS_TOPIC_ARN               = aws_sns_topic.alerts.arn
      DBT_SEND_ANONYMOUS_USAGE_STATS = "False" # avoids ~/.dbt writes on a read-only FS
      HOME                           = "/tmp"
    }
  }

  depends_on = [aws_cloudwatch_log_group.run_dbt]
}
```

In `terraform/alerts.tf`, add to `monitored_lambdas`:

```hcl
    aws_lambda_function.run_dbt.function_name,
```

- [ ] **Step 3: Create the repo, then build & push (chicken-and-egg)**

`data.aws_ecr_image` fails until an image exists, so first apply only the repo (targeted apply is exceptional; this is the documented case):

```bash
cd terraform && terraform apply -target=aws_ecr_repository.run_dbt
```

Then from the repo root:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
aws ecr get-login-password | docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.us-east-1.amazonaws.com
docker build --platform linux/arm64 -f dbt_runner/Dockerfile -t $ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/decentraland-run-dbt:latest .
docker push $ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/decentraland-run-dbt:latest
```

- [ ] **Step 4: Full plan/apply, then smoke-test both paths**

`cd terraform && terraform plan` (read it: lifecycle policy, role, policy, attachment, log group, Lambda, alarm) → `terraform apply`.

Green path:

```bash
aws lambda invoke --function-name run-dbt --payload '{}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: `{"select": "source:bronze.contracts+", "changes": {}}` (cold start with dbt parse can take ~1-2 min). If it errors, read `aws logs tail /aws/lambda/run-dbt --since 10m` — a missing IAM action shows up as an Athena/Glue AccessDenied; add it to the policy above and re-apply.

Red path (also exercises the alarm → red Slack):

```bash
aws lambda invoke --function-name run-dbt --payload '{"select": "nonexistent_model"}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

Expected: function error (`RuntimeError: dbt build failed...`), and within ~5 min the `lambda-errors-run-dbt` alarm posts red to Slack.

- [ ] **Step 5: Commit**

```bash
git add dbt_runner/requirements.txt dbt_runner/Dockerfile .dockerignore terraform/lambda_run_dbt.tf terraform/alerts.tf
git commit -m "feat: run-dbt container-image Lambda (ECR, arm64, dbt-athena)"
```

---

### Task 5: `contracts-on-push` state machine + EventBridge trigger

**Files:**
- Create: `terraform/step_functions_contracts.tf`
- Modify: `terraform/lambda_contracts.tf` (S3 notification → EventBridge; drop direct trigger)

**Interfaces:**
- Consumes: `aws_lambda_function.contracts` (its handler already accepts the S3 `Records` shape — the state synthesizes it, no Lambda code change), `aws_lambda_function.run_dbt`, `aws_sns_topic.alerts`.
- Produces: state machine `contracts-on-push` started by any `landing/contracts/*.csv` object creation.

- [ ] **Step 1: Switch the bucket notification to EventBridge**

In `terraform/lambda_contracts.tf`:
- Delete `resource "aws_lambda_permission" "contracts_s3"` (S3 no longer invokes the Lambda directly).
- Replace the `aws_s3_bucket_notification` resource with:

```hcl
# S3 events now flow through EventBridge: the contracts-on-push state
# machine (step_functions_contracts.tf) starts on landing/contracts/*.csv.
# Future landing/ triggers add EventBridge rules, not lambda_function blocks.
resource "aws_s3_bucket_notification" "lake" {
  bucket      = aws_s3_bucket.lake.id
  eventbridge = true
}
```

- [ ] **Step 2: State machine + rule**

`terraform/step_functions_contracts.tf`:

```hcl
# contracts-on-push: CSV lands in landing/contracts/ -> ingest -> dbt build.
# Event-driven only (no schedule); failures use the custom notify-slack
# contract, same skeleton as dcl-contracts-daily.
locals {
  sfn_contracts_tags = { component = "orchestration", layer = "ops" }
}

resource "aws_iam_role" "sfn_contracts" {
  name = "contracts-on-push-sfn-role"
  tags = local.sfn_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_contracts" {
  name = "invoke-lambdas-publish-alerts"
  role = aws_iam_role.sfn_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = [
          aws_lambda_function.contracts.arn,
          aws_lambda_function.run_dbt.arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "contracts_on_push" {
  name     = "contracts-on-push"
  role_arn = aws_iam_role.sfn_contracts.arn
  tags     = local.sfn_contracts_tags

  definition = jsonencode({
    Comment = "landing/contracts CSV push -> load-contracts -> dbt build"
    StartAt = "LoadContracts"
    States = {
      # Input is the raw EventBridge S3 event; Parameters rebuilds the S3
      # Records shape the Lambda already understands (no handler change).
      LoadContracts = {
        Type     = "Task"
        Resource = aws_lambda_function.contracts.arn
        Parameters = {
          Records = [{
            s3 = {
              bucket = { "name.$" = "$.detail.bucket.name" }
              object = { "key.$" = "$.detail.object.key" }
            }
          }]
        }
        ResultPath = "$.load"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "RunDbt"
      }
      RunDbt = {
        Type       = "Task"
        Resource   = aws_lambda_function.run_dbt.arn
        Parameters = {} # default selector: source:bronze.contracts+
        ResultPath = "$.dbt"
        Retry = [{
          ErrorEquals = [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
          ]
          IntervalSeconds = 5
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        End = true
      }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.alerts.arn
          Message = {
            source            = "step-functions"
            component         = "contracts"
            status            = "FAILED"
            "detail.$"        = "States.Format('{}: {}', $.error.Error, $.error.Cause)"
            "execution_url.$" = "States.Format('https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/{}', $$.Execution.Id)"
          }
        }
        Next = "FailExecution"
      }
      FailExecution = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "A step failed; details were sent to the alerts topic"
      }
    }
  })
}

resource "aws_cloudwatch_event_rule" "contracts_on_push" {
  name        = "contracts-on-push"
  description = "Start contracts-on-push when a CSV lands in landing/contracts/"
  tags        = local.sfn_contracts_tags

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.lake.id] }
      object = { key = [{ wildcard = "landing/contracts/*.csv" }] }
    }
  })
}

resource "aws_iam_role" "eventbridge_sfn_contracts" {
  name = "contracts-on-push-events-role"
  tags = local.sfn_contracts_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_contracts" {
  name = "start-contracts-on-push"
  role = aws_iam_role.eventbridge_sfn_contracts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.contracts_on_push.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "contracts_on_push" {
  rule     = aws_cloudwatch_event_rule.contracts_on_push.name
  arn      = aws_sfn_state_machine.contracts_on_push.arn
  role_arn = aws_iam_role.eventbridge_sfn_contracts.arn
  # Full event passes through: the state machine reads $.detail.bucket/object.
}
```

- [ ] **Step 3: Validate, plan, apply**

```bash
cd terraform && terraform validate && terraform plan
```

Read the plan: expect the notification update, the permission destroy, and the new SFN/EventBridge/IAM resources. Then `terraform apply`.

- [ ] **Step 4: Commit**

```bash
git add terraform/step_functions_contracts.tf terraform/lambda_contracts.tf
git commit -m "feat: contracts-on-push state machine — S3 EventBridge trigger chains ingest + dbt"
```

---

### Task 6: End-to-end verification + docs

**Files:**
- Modify: `CLAUDE.md` (workflow item 2: note the contracts-on-push machine)

**Interfaces:**
- Consumes: everything deployed in Tasks 1-5.

- [ ] **Step 1: Fire the real event**

```bash
aws s3 cp reference/contracts.csv \
  s3://decentraland-data-platform-$(aws sts get-caller-identity --query Account --output text)/landing/contracts/
```

- [ ] **Step 2: Watch the execution**

```bash
SM=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='contracts-on-push'].stateMachineArn" --output text)
aws stepfunctions list-executions --state-machine-arn $SM --max-items 1
# poll until status is SUCCEEDED (LoadContracts ~10s + RunDbt cold start ~2min)
```

Expected: one execution, `SUCCEEDED`. If no execution appears within a minute, the EventBridge rule did not match — check `aws events list-rules --name-prefix contracts-on-push` and the wildcard pattern.

- [ ] **Step 3: Check the data**

Athena (workgroup `decentraland-data-platform`):
`select count(*) from silver.dim_contracts` → **962**.
`select erc_type, count(*) from silver.dim_contracts group by 1 order by 1` → 0=128, 20=2, 721=832 (and no -1 row).

Confirm no red Slack message arrived for this run.

- [ ] **Step 4: Update CLAUDE.md**

In the Workflow section of `CLAUDE.md`, replace the build-order item 2 text

```
2) event-driven
`contracts` ingestion (landing→Lambda→bronze; curated CSV with `dcl_contract` + `erc_type`), 3) on-chain Lambdas
```

with

```
2) event-driven
`contracts` ingestion (DONE — CSV push to landing/contracts/ starts the
`contracts-on-push` state machine: `load-contracts` → `run-dbt`, a
container-image Lambda that builds `silver.dim_contracts` and its tests;
curated CSV carries `dcl_contract` + `erc_type`), 3) on-chain Lambdas
```

- [ ] **Step 5: Final gate + commit**

```bash
.venv/bin/pytest tests/ -q && .venv/bin/ruff check . && (cd dbt && ../.venv/bin/dbt parse) && (cd terraform && terraform validate)
git add CLAUDE.md docs/superpowers/plans/2026-08-23-contracts-on-push.md
git commit -m "docs: contracts-on-push deployed — CSV push now chains ingest + dbt build"
```

Do NOT push; ask the user first.
