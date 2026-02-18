# Allow S3 to invoke the Lambda
resource "aws_lambda_permission" "allow_s3_invoke_etl" {
  statement_id  = "AllowExecutionFromS3InputBucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.etl.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.input.arn
}

# S3 -> Lambda notification for object creates
resource "aws_s3_bucket_notification" "input_bucket_events" {
  bucket = aws_s3_bucket.input.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.etl.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.raw_prefix
    filter_suffix       = var.raw_suffix
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_etl]
}