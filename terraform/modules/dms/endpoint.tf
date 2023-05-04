data "aws_ssm_parameter" "datalake" {
  name = "/platform/${var.env}/common/datalake_bucket"
}

data "aws_ssm_parameter" "datalake_kms" {
  name = "/platform/${var.env}/common/datalake_bucket_key"
}

data "aws_ssm_parameter" "endpoint_password" {
  for_each = local.source_endpoint_config
  name     = "/platform/${var.env}/data_ingestion/${each.value["password"]}"
}

locals {
  datalake_bucket        = data.aws_ssm_parameter.datalake.value
  s3_endpoint_config     = { for k, v in var.endpoint_config : k => v if v["engine"] == "s3" }
  source_endpoint_config = { for k, v in var.endpoint_config : k => v if v["type"] == "source" }
  extra_connection_attributes = {
    sqlserver = {
      ignoreMsReplicationEnablement        = true
      AlwaysOnSharedSynchedBackupIsEnabled = true
      SetUpMsCdcForTables                  = true
    }
  }
}

resource "aws_dms_s3_endpoint" "s3_endpoint" {
  for_each = local.s3_endpoint_config

  endpoint_id             = "${var.prefix}-${local.aws_dms}-${each.value["type"]}-${each.value["engine"]}-${each.key}-${var.env}"
  endpoint_type           = each.value["type"]
  bucket_name             = local.datalake_bucket
  service_access_role_arn = each.value["service_access_role_arn"]

  bucket_folder                     = each.value["path"]
  data_format                       = "parquet"
  date_partition_enabled            = each.value["cdc"]
  date_partition_sequence           = "YYYYMMDD"
  cdc_inserts_and_updates           = each.value["cdc"]
  compression_type                  = "GZIP"
  parquet_timestamp_in_millisecond  = true
  parquet_version                   = "parquet-2-0"
  encryption_mode                   = "SSE_KMS"
  server_side_encryption_kms_key_id = data.aws_ssm_parameter.datalake_kms.value
}

resource "aws_dms_endpoint" "source_endpoint" {
  for_each = local.source_endpoint_config

  endpoint_id                 = "${var.prefix}-${local.aws_dms}-${each.value["type"]}-${each.value["engine"]}-${each.key}-${var.env}"
  endpoint_type               = each.value["type"]
  engine_name                 = each.value["engine"]
  server_name                 = each.value["server_name"]
  port                        = each.value["port"]
  username                    = each.value["username"]
  password                    = data.aws_ssm_parameter.endpoint_password[each.key].value
  database_name               = each.value["database_name"]
  extra_connection_attributes = join(";", [for k, v in local.extra_connection_attributes[each.value["engine"]] : "${k}=${v}"])
}