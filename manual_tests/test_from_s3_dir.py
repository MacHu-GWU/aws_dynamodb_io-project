# -*- coding: utf-8 -*-

import boto3
from aws_dynamodb_io.api import ExportJob
from rich import print as rprint

s3_client = boto3.Session(profile_name="bmt_app_dev_us_east_1").client("s3")
bucket = "bmt-app-dev-us-east-1-data"
prefix = "projects/parquet_dynamodb/dataset/simple/export/AWSDynamoDB/01725162280092-940349cc/"
export_job = ExportJob.from_s3_dir(
    s3_client=s3_client,
    bucket=bucket,
    prefix=prefix,
)
rprint(export_job)
