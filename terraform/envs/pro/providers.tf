terraform {
  required_version = "1.3.8"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.61"
    }
  }
}

provider "aws" {
  region              = local.aws_region
  allowed_account_ids = [local.account_id]
}
