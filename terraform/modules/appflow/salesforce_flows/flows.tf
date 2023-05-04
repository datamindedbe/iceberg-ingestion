locals {
  ingestion_type = try(var.config["type"], "full")
}

resource "aws_appflow_flow" "salesforce_flow" {
  name = "salesforce-${var.name}-${var.env}"

  source_flow_config {
    connector_type         = "Salesforce"
    connector_profile_name = var.salesforce_connector_profile_name
    source_connector_properties {
      salesforce {
        object                      = var.config["source"]
        enable_dynamic_field_update = true
        include_deleted_records     = true
        # TODO: Support for data transferAPI is not part of a released terraform provider yet
        # data_transfer_api = "AUTOMATIC"
      }
    }
    dynamic "incremental_pull_config" {
      for_each = local.ingestion_type == "incremental" ? [1] : []
      content {
        datetime_type_field_name = "SystemModstamp"
      }
    }
  }

  destination_flow_config {
    connector_type = "S3"
    destination_connector_properties {
      s3 {
        bucket_name   = var.destination_bucket
        bucket_prefix = "raw/salesforce"
        s3_output_format_config {
          file_type                   = "PARQUET"
          preserve_source_data_typing = true

          aggregation_config {
            aggregation_type = "None"
          }
          prefix_config {
            prefix_type = "PATH"
          }
        }
      }
    }
  }

  task {
    task_type     = "Map_all"
    source_fields = [""]
    task_properties = {
      EXCLUDE_SOURCE_FIELDS_LIST = "[]"
    }
    connector_operator {
      salesforce = "NO_OP"
    }
  }

  trigger_config {
    trigger_type = local.ingestion_type == "incremental" ? "Scheduled" : "OnDemand"
    dynamic "trigger_properties" {
      for_each = local.ingestion_type == "incremental" ? [1] : []
      content {
        scheduled {
          schedule_expression = "rate(1days)"
          data_pull_mode      = "Incremental"
          # Set to 10 minutes in the future, should be fine for manually enabling it as this can't be done via terraform
          # Should always be a date in the future, which is quite annoying to automate
          schedule_start_time = timeadd(timestamp(), "10m")
          timezone            = "CET"
        }
      }
    }
  }

  # TODO: Task is update after the fact so terraform state gets confused.
  # TODO: Trigger config will be updated every time otherwise as timestamp always changes
  lifecycle {
    ignore_changes = [task, trigger_config]
  }
}