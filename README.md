# External Search Keyword Revenue Attribution

Daily serverless pipeline that attributes revenue to search engine keywords using a last-touch model.

Implements the take-home exercise as:
- A small, testable Python application (external_search_revenue)
- A serverless ETL on AWS (S3 + Lambda)
- Terraform for infrastructure
- GitHub Actions for CI/CD

---

## 1. Business Problem

Marketing teams invest heavily in search (organic SEO + paid ads). They can measure clicks and sessions, but the real question is attributed purchase revenue per keyword.

**This project answers:**
- Which external search engines and keywords actually drive purchase revenue (not just visits)?
- Where should we increase vs. reduce search investment based on revenue impact?
- Which keywords look inefficient (high traffic, low revenue) and need optimization or pausing?

**Outcome / Value:**
- Identifies top-performing keywords and engines by revenue
- Enables smarter budget/campaign optimization decisions
- Produces a daily keyword revenue table that can feed dashboards or a data warehouse

---

## 2. Input Data & Attribution Model

**Input data**

The pipeline expects a TSV (tab-separated) hit log with at least:
- `ip`
- `user_agent`
- `referrer`
- `event_list`
- `product_list`

**Example product_list:**
```
Electronics;Ipod;1;290;,Electronics;Case;1;10;
```

**Key assumptions:**
- **Visitor identity:** `visitor_id = ip + user_agent`
- **Purchase definition:** `event_list` contains "1" then this row is a purchase event. Other events (e.g., "2" = product view) do not count as purchases.
- **Revenue:** From `product_list`:
  - Each product item is `category;product_name;quantity;revenue;...`
  - The pipeline uses the 4th field (revenue) and sums across items in that hit.

---

## 3. Attribution Logic

All implemented in `src/external_search_revenue/analyzer.py` via `ExternalSearchRevenueAnalyzer`.

### 3.1 Visitor & last-touch model

- **Visitor** = `ip + user_agent`
- For each visitor, the analyzer maintains the last external search seen:
  - Extracted from the referrer URL
  - Yields `(search_engine_domain, search_keyword)`
- **Last-touch rule:** A purchase is attributed to the most recent external search seen before that purchase for the same visitor.
- **Internal vs external:**
  - Hosts on `esshopzilla.com` (and subdomains) are treated as internal navigation, not search.
  - Only non-internal hosts can be considered eligible search engines.

### 3.2 Interpreting search referrers

For each referrer:

1. Parse as URL (`urllib.parse.urlparse`)
2. Extract host + query string
3. If host is internal (`esshopzilla.com` or subdomain) then ignore
4. Detect search engines by base domain:
   - `google.com`, `bing.com`, `yahoo.com`, `msn.com`, etc.
5. Map to keyword parameter:
   - `google.com`, `bing.com`, `msn.com` → `q`
   - `yahoo.com` → `p`
   - If mapping fails, try generic keys `["q", "p"]`
6. Normalize keyword:
   - URL-decode (`%20`, `+` to spaces)
   - Collapse whitespace
   - Lowercase
   - Example: `"Ipod Nano"` → `"ipod nano"`

If any step fails (bad URL, missing query, no keyword), the referrer is ignored.

### 3.3 Revenue aggregation

On each purchase row:

1. Look up the visitor's last external search; if none:
   - Count the purchase, but do not attribute revenue to any keyword.
2. Parse `product_list` and sum revenue:
   - Use the 4th field of each product as the revenue value.
   - If parsing fails, skip that and continue.
3. Aggregate revenue by `(engine_domain, keyword)`:
```python
{ KeywordAttribution(engine_domain="google.com", keyword="ipod"): 290.0, ...}
```

---

## 4. Output Schema

The core ETL writes a TSV report with:
- Search Engine Domain
- Search Keyword
- Revenue

**Example row:**
```
google.com	ipod	290.00
```

Each row represents total attributed revenue for one (engine, keyword) pair for that run.

---

## 5. Repository Structure
```
external-search-revenue/
├── docs/
│   └── SearchKeywordRevenue_Presentation.pptx   # Solution overview + architecture + scaling
├── infra/
│   ├── provider.tf              # Terraform provider & region
│   ├── s3.tf                    # S3 input/output buckets + tags
│   ├── s3_notifications.tf      # S3 → Lambda notifications
│   ├── iam.tf                   # IAM role & policy for Lambda
│   ├── lambda.tf                # Lambda function definition
│   └── handler.py               # AWS Lambda handler
├── src/
│   └── external_search_revenue/
│       ├── __init__.py
│       ├── analyzer.py          # Core attribution logic
│       └── main.py              # Local CLI wrapper around the core analyzer
├── tests/
│   └── test_analyzer.py         # Unit tests for the analyzer
├── .github/
│   └── workflows/
│       └── deploy-lambda.yml    # CI/CD: build & deploy Lambda
├── README.md
└── Makefile                     # Convenience commands (tests, etc.)
```

---

## 6. Python Application

### 6.1 Analyzer (src/external_search_revenue/analyzer.py)

**Key types:**

- **KeywordAttribution** `(engine_domain: str, keyword: str)`
  - Frozen dataclass; used as the key for aggregation.
- **AnalyzerCounters**
  - Tracks counters for observability:
    - `rows_seen`
    - `search_referrers_seen`
    - `purchases_seen`
    - `purchases_attributed`
    - `revenue_attributed`
    - `bad_revenue_values`
    - `purchases_missing_prior_search`

**Core class:**

**ExternalSearchRevenueAnalyzer**

- `run(tsv_path: Union[str, Path]) -> Dict[KeywordAttribution, float]`
  - Local/CLI method. Opens a TSV from disk, builds a `csv.DictReader`, and delegates to `_process_rows(...)`. Used for local runs.
- `run_rows(rows: Iterable[Mapping[str, str]]) -> Dict[KeywordAttribution, float]`
  - Streaming-friendly entrypoint. Accepts any iterable of row dicts (e.g., `csv.DictReader` over an S3 `TextIOWrapper`) and delegates to `_process_rows(...)`. Used by Lambda.
- `ranked_rows(totals: Dict[KeywordAttribution, float]) -> List[Tuple[KeywordAttribution, float]]`
  - Returns a list of `(KeywordAttribution, revenue)` sorted by revenue descending.
- `default_output_filename() -> str`
  - Returns `YYYY-MM-DD_SearchKeywordPerformance.tab` (based on execution date).

**Internal single-pass logic:** `_process_rows(...)`

Maintains two pieces of state during one scan:
- `visitor_key -> last_touch (engine_domain, keyword)` (last external search seen for that visitor)
- `(engine_domain, keyword) -> revenue_total` (aggregated attributed revenue)

> Both `run()` and `run_rows()` share the same core attribution logic via `_process_rows()`; `run()` is just a local file wrapper, while `run_rows()` is the streaming interface used in AWS Lambda.

### 6.2 Local CLI (src/external_search_revenue/main.py)

You can run the analyzer locally against a TSV file:
```bash
python -m external_search_revenue.main path/to/hit_data.tsv
```

**Behavior:**
- Exactly one argument: the input TSV path.
- Output is written into the output directory as: `YYYY-MM-DD_SearchKeywordPerformance.tab`

---

## 7. Unit Tests

`tests/test_analyzer.py` focuses on correctness of the core logic.

It covers:

- **Keyword normalization:** `"Ipod"` and `"ipod"` roll up into `"ipod"`.
- **Last-touch attribution (per visitor = ip + user_agent):** Same visitor searches twice (ipod on Google, then zune on Bing); purchase is attributed to Bing / zune.
- **Internal vs external referrers:** Referrers on `esshopzilla.com` are treated as internal. If a purchase occurs after only internal referrers, it is not attributed to any keyword.
- **Engine-specific parameters:** Validates that Yahoo keywords are extracted from `p=` whereas `q=` for Google/Bing/MSN.
- **Multiple products in one purchase:** Confirms revenue from all products in `product_list` is summed and attributed correctly.
- **Non-purchase events:** Rows where `event_list` does not contain "1" (e.g. "2") are ignored.
- **Ranking helper:** `ranked_rows(...)` returns results sorted by revenue in descending order.

> Implementation note: tests use a small helper to write a minimal TSV containing only the required columns to keep the scenarios focused.

**Run tests with:**
```bash
make test
```

or
```bash
PYTHONPATH=src pytest -q
```

---

## 8. AWS Architecture (Streaming S3 → Lambda → S3)

### 8.1 High-level design

- **Input:** S3 bucket for hit logs (e.g., `external-search-revenue-input`)
- **Output:** S3 bucket for reports (e.g., `external-search-revenue-output`)
- **Compute:** AWS Lambda `external-search-revenue-etl`
- **Runtime:** Python 3.11
- **IAM (least privilege):**
  - Read input objects: `s3:GetObject` on `external-search-revenue-input/*`
  - Write output objects: `s3:PutObject` on `external-search-revenue-output/*`
  - CloudWatch logs: permissions to create log groups/streams and write logs
- **Triggers:** S3 ObjectCreated events on the input bucket invoke the Lambda.

### 8.2 Streaming handler (infra/handler.py)

The Lambda entrypoint is defined in `infra/handler.py` as:

**`handler = "handler.lambda_handler"`**

in `lambda.tf`, so AWS invokes the `lambda_handler()` function from `handler.py` in the deployed Lambda package.

**The Lambda handler:**

1. Reads env vars:
   - `INPUT_BUCKET`
   - `OUTPUT_BUCKET`
2. Validates that the S3 event came from the expected `INPUT_BUCKET`. If not, logs and exits.
3. Calls `s3.get_object` and wraps the body in `io.TextIOWrapper`:
   - No `/tmp` files; the TSV is streamed directly from S3.
4. Uses `csv.DictReader` over the stream and passes it to:
   - `ExternalSearchRevenueAnalyzer.run_rows(reader)`
5. Calls `analyzer.ranked_rows(totals)` to sort by revenue.
6. Builds the output TSV in memory (`io.StringIO`):
   - Header: `Search Engine Domain, Search Keyword, Revenue`
7. Uploads the report to the output bucket via `put_object`:
   - Key: `reports/<YYYY-MM-DD>_SearchKeywordPerformance.tab`
8. **Observability (CloudWatch):**
   - CloudWatch Logs: structured log lines for `input_bucket/key`, `rows_processed`, `purchases_seen`, `purchases_attributed`, `output_key`.
   - Lambda metrics (default): Invocations, Errors, Duration, Throttles, ConcurrentExecutions.

This design keeps Lambda stateless and streaming-friendly.

---

## 9. Infrastructure as Code (Terraform)

**Under infra/:**

### 1. provider.tf
- Configures the AWS provider and region (via var.aws_region) and applies default_tags (Project/Environment/Owner) to all resources.

### 2. s3.tf
- Creates:
  - Input bucket (e.g., `external-search-revenue-input`)
  - Output bucket (e.g., `external-search-revenue-output`)
  Applies:
  - S3 Public Access Block (blocks all public access)
  - Default encryption SSE-KMS using the key from kms.tf

### 3. iam.tf
- IAM assume-role policy for Lambda (`lambda.amazonaws.com`).
- Inline policy (least privilege) that allows:
  - **CloudWatch Logs:** `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
  - **Input bucket (read):** `s3:GetObject` on `external-search-revenue-input/*`
  - **Output bucket (write):** `s3:PutObject` on `external-search-revenue-output/*`
  - **KMS permissions** for SSE-KMS buckets: kms:Decrypt, kms:Encrypt, kms:GenerateDataKey, kms:DescribeKey

### 4. lambda.tf
- Defines `aws_lambda_function` "etl":
  - `function_name = "external-search-revenue-etl"`
  - `runtime = "python3.11"`
  - `handler = "handler.lambda_handler"`
  - Environment variables: `INPUT_BUCKET`, `OUTPUT_BUCKET`, `OUTPUT_PREFIX`, `RAW_PREFIX` , `REPORT_TZ`
  - Timeout and memory sized for the workload (Lambda max runtime is 15 minutes).
- Terraform manages infrastructure; **code deployment** is handled by GitHub Actions.

### 5. s3_notifications.tf
- `aws_lambda_permission.allow_s3_invoke_etl`
  - Allows S3 to call `lambda:InvokeFunction` on the ETL Lambda.
- `aws_s3_bucket_notification.input_bucket_events`
  - Configures S3 → Lambda trigger for `s3:ObjectCreated:*`
  - Filters:
		filter_prefix = var.raw_prefix (e.g., raw/)
		filter_suffix = var.raw_suffix (e.g., .tsv)
  - Uses `depends_on` so Lambda permission exists before notification setup

### 6.  variables.tf
- Centralizes configuration:
	-	aws_region, environment
	-	Lambda sizing: lambda_timeout, lambda_memory_mb
	-	Trigger filters: raw_prefix, raw_suffix
	-	Output behavior: output_prefix
	-	Report naming timezone: report_tz

### 7. kms.tf
- Creates a dedicated AWS KMS key (SSE-KMS) for S3 encryption:
	-	Key rotation enabled
	-	Alias: alias/<project>-<env>-s3

---

## 10. CI/CD (GitHub Actions)

The GitHub Actions workflow (`.github/workflows/deploy-lambda.yml`) automates Lambda code deployments.

**When it runs:**
- On pushes to `main` that change `infra/handler.py` or `src/external_search_revenue/`
- Also supports manual `workflow_dispatch`

**What it does:**
- Assumes an AWS role via OIDC (no long-lived access keys)
- Packages `handler.py` + the `external_search_revenue` Python module into a zip
- Deploys the zip to the existing Lambda function using `aws lambda update-function-code`

> Terraform remains the source of truth for infrastructure under `infra/`; GitHub Actions handles code-only rollouts.

---

## 11. Running End-to-End

### 1. Provision infrastructure (Terraform)

From `infra/`:
```bash
terraform init
terraform apply
```

This creates:
- Input + output S3 buckets
- Lambda function
- IAM role/policies
- S3 → Lambda notification trigger (ObjectCreated)

### 2. Ensure Lambda code is up to date (CI/CD)

- Merge changes to `main`
- GitHub Actions workflow (`.github/workflows/deploy-lambda.yml`) packages and deploys the latest code to Lambda

### 3. Upload a test TSV to the input bucket

Example key: `raw/2026-02-16/hit_data.tsv`

Using AWS CLI:
```bash
aws s3 cp data/hit_data.tsv \
  s3://external-search-revenue-input/raw/$(date +%F)/hit_data.tsv
```

This upload automatically triggers the `external-search-revenue-etl` Lambda via the S3 ObjectCreated event.

### 4. Watch CloudWatch Logs

Open the log group for `external-search-revenue-etl` and verify the summary counters:
- `rows_seen` — total rows processed
- `search_referrers_seen` — external search referrer hits detected
- `purchases_seen` — purchase events encountered
- `purchases_attributed` — purchases successfully attributed to a prior external search
- `revenue_attributed` — total attributed revenue (USD)
- `bad_revenue_values` — malformed/invalid revenue fields encountered
- `purchases_missing_prior_search` — purchases with no prior external search touch (not attributed)

Confirm the run completed without errors and the output key was written to the reports bucket.

### 5. Verify output

In the output bucket under `reports/`:
- `reports/<YYYY-MM-DD>_SearchKeywordPerformance.tab`
- Open the file and spot-check a few rows (engine domain, keyword, revenue totals).

---

## 12. Scalability, Limitations & Next Steps

### Scalability to 10GB+ files

- The core algorithm is single-pass and streaming.
- Memory grows with `O(unique_visitors + unique(engine, keyword))`, not raw file size.
- For very large files, the practical ceiling is Lambda runtime and memory:
  - A single Lambda processing a 10GB TSV may approach the 15-minute timeout limit.
  - Visitor state (`visitor_key -> last_touch`) can grow large for high-cardinality traffic.

### For true 10GB+ workloads

- **Parallelize safely (stateful sharding):**
  - Shard upstream by `hash(visitor_key)` so all events for a visitor land in the same partition, then process partitions in parallel.
- **Use a batch/distributed engine:**
  - Run the same logic in AWS Glue (Spark) / EMR / ECS Fargate, orchestrated by Step Functions / MWAA / EventBridge.
- **Control state growth:**
  - Add TTL/eviction for inactive visitors, or externalize last-touch state (e.g., DynamoDB with TTL) when required.

### Current limitations

- Visitor identity = `ip + user_agent` only (no cross-device identity).
- Attribution model is last-touch only.
- Correctness is validated via unit tests and small samples, formal load/performance tests on full 10GB files are not included.
- No dashboard shipped; the output is designed to plug into BI or a warehouse.

### Potential enhancements

- Add a scheduler (EventBridge / Step Functions / MWAA) for strict daily SLAs.
- Add CloudWatch dashboards & alerts for volume/revenue anomalies.
- Load outputs into a warehouse/lake and connect to BI (e.g., Athena/Glue Catalog + QuickSight, Looker, Power BI).

---

## 13. Business Presentation Deck

For the code review / walkthrough call, there is a business and architecture presentation that explains:
- The problem statement and stakeholder questions
- Attribution logic and input assumptions
- AWS serverless architecture (S3 → Lambda → S3)
- Engineering practices, scalability considerations, and next steps

**Deck:** `docs/SearchKeywordRevenue_Presentation.pptx`