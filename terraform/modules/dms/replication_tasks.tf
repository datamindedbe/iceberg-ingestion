data "aws_ssm_parameter" "replication_instance_arn" {
  name = "/platform/${var.env}/common/dms_replication_instance_arn"
}

module "replication_tasks" {
  source   = "./replication_tasks"
  for_each = var.dms_flows

  aws_region = var.aws_region
  prefix     = var.prefix
  env        = var.env

  replication_instance_arn      = data.aws_ssm_parameter.replication_instance_arn.value
  full_destination_endpoint_arn = aws_dms_s3_endpoint.s3_endpoint["${each.key}-full"]["endpoint_arn"]
  cdc_destination_endpoint_arn  = aws_dms_s3_endpoint.s3_endpoint["${each.key}-cdc"]["endpoint_arn"]
  source_endpoint_arn           = aws_dms_endpoint.source_endpoint[each.key].endpoint_arn

  source_system = each.key
  tasks         = each.value
}