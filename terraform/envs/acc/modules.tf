locals {
  appflow_flows = yamldecode(file("${path.root}/config/appflow_flows.yaml"))
  dms_config    = yamldecode(file("${path.root}/config/dms_config.yaml"))
  dms_flows     = yamldecode(file("${path.root}/config/dms_flows.yaml"))
}

module "appflow" {
  source = "../../modules/appflow"

  aws_region          = local.aws_region
  prefix              = local.prefix
  env                 = local.env
  appflow_flows       = local.appflow_flows
  appflow_bucket_name = local.appflow_bucket_name

  salesforce_instance_url = local.salesforce_instance_url
}

module "dms" {
  source = "../../modules/dms"

  aws_region               = local.aws_region
  prefix                   = local.prefix
  env                      = local.env
  endpoint_config          = local.dms_config["endpoints"]
  dms_flows                = local.dms_flows
}