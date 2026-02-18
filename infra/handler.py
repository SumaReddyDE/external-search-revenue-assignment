"""AWS Lambda handler for the external search revenue pipeline.

This function is designed to be triggered by an S3 event on the input bucket:

- INPUT_BUCKET: S3 bucket where hit-data TSV files are uploaded.
- OUTPUT_BUCKET: S3 bucket where the aggregated report will be written.

Implementation note:
--------------------
Here we stream the input directly from S3, pass rows into
ExternalSearchRevenueAnalyzer.run_rows, and then write the final report
straight back to S3.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from typing import Any, Dict

import boto3

from external_search_revenue.analyzer import ExternalSearchRevenueAnalyzer

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

logging.getLogger("external_search_revenue.analyzer").setLevel(logging.INFO)

_s3 = boto3.client("s3")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entrypoint for S3 triggered external search revenue attribution."""
    input_bucket_expected = os.environ["INPUT_BUCKET"]
    output_bucket = os.environ["OUTPUT_BUCKET"]

    raw_prefix = os.environ.get("RAW_PREFIX", "").lstrip("/")  # e.g. "raw/"

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    if bucket != input_bucket_expected:
        LOG.warning(
            "Received event for unexpected bucket %s (expected %s); skipping.",
            bucket,
            input_bucket_expected,
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Ignored event for unexpected bucket.",
                    "bucket": bucket,
                    "key": key,
                }
            ),
        }

    # Skip if key doesn't match expected raw prefix
    if raw_prefix and not key.startswith(raw_prefix):
        LOG.info(
            "Skipping object with key %s because it does not match RAW_PREFIX=%s",
            key,
            raw_prefix,
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Ignored object outside RAW_PREFIX.",
                    "bucket": bucket,
                    "key": key,
                    "raw_prefix": raw_prefix,
                }
            ),
        }

    LOG.info("Starting Lambda for object s3://%s/%s", bucket, key)

    # 1) Stream the TSV from S3
    obj = _s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]  # botocore.response.StreamingBody

    # Wrap the streaming body as text so csv.DictReader can consume it line by line
    text_stream = io.TextIOWrapper(body, encoding="utf-8")
    reader = csv.DictReader(text_stream, delimiter="\t")

    analyzer = ExternalSearchRevenueAnalyzer()
    totals = analyzer.run_rows(reader)

    # 2) Sort rows by revenue descending
    sorted_rows = analyzer.ranked_rows(totals)

    # 3) Build the TSV report in memory
    buffer = io.StringIO()
    buffer.write("Search Engine Domain\tSearch Keyword\tRevenue\n")
    for attribution, revenue in sorted_rows:
        buffer.write(
            f"{attribution.engine_domain}\t{attribution.keyword}\t{revenue:.2f}\n"
        )

    report_body = buffer.getvalue()

    report_tz = os.environ.get("REPORT_TZ", "America/Chicago")
    report_filename = analyzer.default_output_filename(report_tz)

    output_prefix = os.environ.get("OUTPUT_PREFIX", "reports/").rstrip("/") + "/"
    output_key = f"{output_prefix}{report_filename}"

    LOG.info(
        "Uploading report to s3://%s/%s (rows=%d)",
        output_bucket,
        output_key,
        len(sorted_rows),
    )

    _s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=report_body.encode("utf-8"),
        ContentType="text/tab-separated-values",
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "External search revenue report generated.",
                "input_bucket": bucket,
                "input_key": key,
                "output_bucket": output_bucket,
                "output_key": output_key,
                "rows": len(sorted_rows),
            }
        ),
    }
