variable "environment" {
  description = "Deployment environment (e.g. dev, stage, prod)"
  type        = string
  default     = "dev"
}

# Lambda timeout in seconds
variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300
}

# Lambda memory in MB
variable "lambda_memory_mb" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 512
}

variable "aws_region" {
  description = "AWS region to deploy the external-search-revenue stack"
  type        = string
  default     = "us-east-1"
}

variable "raw_prefix" {
  description = "S3 key prefix that triggers the Lambda"
  type        = string
  default     = "raw/"
}

variable "output_prefix" {
  description = "Prefix in the output bucket for generated reports"
  type        = string
  default     = "reports/"
}

variable "report_tz" {
  description = "Timezone used for report execution date"
  type        = string
  default     = "America/Chicago"
}

variable "raw_suffix" {
  description = "S3 object suffix that triggers the Lambda (e.g., .tsv)"
  type        = string
  default     = ".tsv"
}