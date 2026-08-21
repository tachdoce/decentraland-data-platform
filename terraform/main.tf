terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      project    = "decentraland-data-platform"
      managed_by = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  bucket_name = "decentraland-data-platform-${data.aws_caller_identity.current.account_id}"
  base_tags   = { component = "platform", layer = "infra" }
}
