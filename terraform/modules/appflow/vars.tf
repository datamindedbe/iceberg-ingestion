variable "aws_region" {
  description = "The aws region to be used to create all terraform resources"
  type        = string
}
variable "prefix" {}
variable "env" {}
variable "salesforce_instance_url" {}
variable "appflow_flows" {
  description = "the dictionary of flows to be created"
}
variable "appflow_bucket_name" {}