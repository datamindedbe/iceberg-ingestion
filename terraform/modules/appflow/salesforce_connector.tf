data "aws_ssm_parameter" "salesforce_refresh_token" {
  name = "/project/${var.env}/dataingestion/salesforce_refresh_token"
}

data "aws_ssm_parameter" "salesforce_access_token" {
  name = "/project/${var.env}/dataingestion/salesforce_access_token"
}

resource "aws_appflow_connector_profile" "salesforce" {
  name            = "${var.prefix}-${local.aws_app_flow}-salesforce-${var.env}"
  connector_type  = "Salesforce"
  connection_mode = "Public"


  connector_profile_config {
    connector_profile_credentials {
      salesforce {
        access_token  = data.aws_ssm_parameter.salesforce_access_token.value
        refresh_token = data.aws_ssm_parameter.salesforce_refresh_token.value
      }
    }
    connector_profile_properties {
      salesforce {
        instance_url           = var.salesforce_instance_url
        is_sandbox_environment = false
      }
    }
  }
}

module "salesforce_flows" {
  source   = "./salesforce_flows"
  for_each = var.appflow_flows["salesforce"]

  aws_region = var.aws_region
  env        = var.env
  prefix     = var.prefix

  name   = each.key
  config = each.value

  salesforce_connector_profile_name = aws_appflow_connector_profile.salesforce.name

  destination_bucket = var.appflow_bucket_name
}
