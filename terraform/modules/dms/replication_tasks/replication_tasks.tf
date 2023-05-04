resource "random_integer" "rule_id" {
  for_each = var.tasks

  min = 0
  max = 64 * 1024 * 1024
}

resource "aws_dms_replication_task" "replication_tasks" {
  for_each = var.tasks

  replication_task_id = "${var.prefix}-${local.aws_dms}-${var.source_system}-${each.key}-${var.env}"
  migration_type      = try(each.value["type"], "full") == "cdc" ? "full-load-and-cdc" : "full-load"
  table_mappings = jsonencode({
    rules = [
      {
        rule-type = "selection"
        rule-name = "${var.prefix}-${local.aws_dms}-${var.source_system}-${each.key}-task-${var.env}"
        rule-id   = random_integer.rule_id[each.key].result
        object-locator = {
          schema-name = each.value["schema"]
          table-name  = each.value["source"]
        }
        rule-action = "include"
        filters     = []
      }
    ]
  })

  replication_instance_arn = var.replication_instance_arn
  source_endpoint_arn      = var.source_endpoint_arn
  target_endpoint_arn      = try(each.value["type"], "full") == "cdc" ? var.cdc_destination_endpoint_arn : var.full_destination_endpoint_arn
  start_replication_task   = try(each.value["type"], "full") == "cdc" ? true : false

  lifecycle {
    ignore_changes = [replication_task_settings]
  }
}