variable "aws_region" {
  description = "The aws region to be used to create all terraform resources"
  type        = string
}
variable "prefix" {}
variable "env" {}

variable "name" {}
variable "config" {}

variable "salesforce_connector_profile_name" {}

variable "destination_bucket" {}