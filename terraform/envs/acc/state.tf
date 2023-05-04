terraform {
  backend "s3" {
    # TODO migrate to /test/
    key = "terraform/projects/data_ingestion/acc/state.tfstate"

    region         = "eu-west-3"
    bucket         = "terraformstatebucket"
    kms_key_id     = "arn:aws:kms:eu-west-3:123456789012:key/1ed25f95-fd47-40af-aec5-aaaaaaaaaaaaaaaa"
    encrypt        = true
    dynamodb_table = "terraform-states"
  }
}
