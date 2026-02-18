resource "aws_lambda_function" "etl" {
  function_name = "external-search-revenue-etl"

  description = "Daily external search keyword revenue attribution (serverless ETL)"

  role    = aws_iam_role.etl_lambda_role.arn
  runtime = "python3.11"
  handler = "handler.lambda_handler"

  # Initial bootstrap zip created under infra/
  filename         = "${path.module}/lambda_stub.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda_stub.zip")

  # Tunable via variables (defaults in variables.tf)
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_mb
  architectures = ["x86_64"]

  environment {
    variables = {
      INPUT_BUCKET  = aws_s3_bucket.input.bucket
      OUTPUT_BUCKET = aws_s3_bucket.output.bucket
      OUTPUT_PREFIX = var.output_prefix
      REPORT_TZ     = var.report_tz
      RAW_PREFIX    = var.raw_prefix
    }
  }
}