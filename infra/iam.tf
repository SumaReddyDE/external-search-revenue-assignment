data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Who am I? (used for account_id in ARNs)
data "aws_caller_identity" "current" {}

# IAM role assumed by the external-search-revenue-etl Lambda function
resource "aws_iam_role" "etl_lambda_role" {
  name               = "external-search-etl-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Permissions for Lambda: logs + S3 input/output + KMS (SSE-KMS)
data "aws_iam_policy_document" "etl_lambda_policy" {
  # CloudWatch Logs permissions
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  # Read from input bucket
  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.input.arn,
      "${aws_s3_bucket.input.arn}/*",
    ]
  }

  # Write to output bucket
  statement {
    effect = "Allow"

    actions = [
      "s3:PutObject",
    ]

    resources = [
      "${aws_s3_bucket.output.arn}/*",
    ]
  }

  # Allow Lambda to use the KMS key used for S3 SSE-KMS encryption
  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]

    resources = [
      aws_kms_key.s3_key.arn
    ]
  }
}

# Attach the inline policy to the role
resource "aws_iam_role_policy" "etl_lambda_role_policy" {
  name   = "external-search-etl-policy"
  role   = aws_iam_role.etl_lambda_role.id
  policy = data.aws_iam_policy_document.etl_lambda_policy.json
}