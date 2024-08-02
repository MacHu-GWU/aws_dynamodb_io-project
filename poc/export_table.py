# -*- coding: utf-8 -*-

import typing as T
import time
from pathlib import Path
from datetime import datetime, timezone

import polars as pl
import pynamodb_mate.api as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager
from amazon.ion.simple_types import IonPyDict

import aws_dynamodb_io.api as dio


# ------------------------------------------------------------------------------
# **Update the following variables**
# ------------------------------------------------------------------------------
aws_profile = "bmt_app_dev_us_east_1"
table_name = "aws_dynamodb_io-export_table_example-person"
export_format = dio.ExportFormatEnum.ION  # ION | DYNAMODB_JSON
# export_format = dio.ExportFormatEnum.DYNAMODB_JSON  # ION | DYNAMODB_JSON


# ------------------------------------------------------------------------------
# Don't change the code below
# ------------------------------------------------------------------------------
bsm = BotoSesManager(profile_name=aws_profile)
aws_region = bsm.aws_region
aws_account_id = bsm.aws_account_id

s3dir_root = S3Path(
    f"s3://{bsm.aws_account_alias}-{aws_region}-data/projects/aws_dynamodb_io/poc"
).to_dir()
s3dir_exports = s3dir_root.joinpath("exports").to_dir()

table_arn = f"arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/{table_name}"


class Bio(pm.MapAttribute):
    dob = pm.UnicodeAttribute(null=True)
    address = pm.UnicodeAttribute(null=True)
    hobby = pm.UnicodeAttribute(null=True)


class Relationship(pm.MapAttribute):
    name = pm.UnicodeAttribute()
    relation = pm.UnicodeAttribute()


class Person(pm.Model):
    class Meta:
        table_name = table_name
        region = aws_region
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    id = pm.NumberAttribute(hash_key=True)
    name = pm.UnicodeAttribute()
    height = pm.NumberAttribute(null=True)
    weight = pm.NumberAttribute(null=True)
    shoe_size = pm.UnicodeAttribute(null=True)
    bio = Bio()
    relationships = pm.ListAttribute(of=Relationship)


def step1_create_table():
    Person.create_table(wait=True)
    time.sleep(1)
    bsm.dynamodb_client.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )


def step2_create_dummy_data():
    Person.delete_all()

    records = [
        {
            "id": 1,
            "name": "Alice",
            "height": 5.2,
            "weight": 96,
            "shoe_size": None,
            "bio": {
                "dob": "1990-01-01",
                "address": "123 Main St.",
                "hobby": None,
            },
            "relationships": [
                {"name": "Bob", "relation": "friend"},
                {"name": "Charlie", "relation": "father"},
            ],
        },
    ]

    with Person.batch_write() as batch:
        for record in records:
            person = Person()
            person.from_simple_dict(record)
            # print(person)  # for debug only
            batch.save(person)


def get_utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def step3_export_table() -> str:
    export_time = get_utc_now()
    export_job = dio.ExportJob.export_table_to_point_in_time(
        dynamodb_client=bsm.dynamodb_client,
        table_arn=table_arn,
        s3_bucket=s3dir_exports.bucket,
        s3_prefix=s3dir_exports.key,
        export_format=export_format.value,
        export_time=export_time,
    )
    export_arn = export_job.arn
    print(f"{export_arn = }")
    print(f"preview exported data: {S3Path(export_job.s3uri_export).console_url}")
    return export_arn


def step4_wait_export_complete(export_arn: str):
    dio.ExportJob.wait_until_complete(
        dynamodb_client=bsm.dynamodb_client,
        export_arn=export_arn,
    )


def step5_check_export_results(export_arn: str):
    # wait until the export to complete
    export_job = dio.ExportJob.describe_export(
        dynamodb_client=bsm.dynamodb_client,
        export_arn=export_arn,
    )

    # iterate over the records
    kwargs = dict(dynamodb_client=bsm.dynamodb_client, s3_client=bsm.s3_client)
    if export_format is dio.ExportFormatEnum.ION:
        records = list(export_job.read_amazon_ion(**kwargs))
    elif export_format is dio.ExportFormatEnum.DYNAMODB_JSON:
        records = list(export_job.read_dynamodb_json(**kwargs))
    else:  # pragma: no cover
        raise NotImplementedError

    print(records[0])


if __name__ == "__main__":
    """
    Execute steps one by one.
    """
    # step1_create_table()
    # step2_create_dummy_data()
    # export_arn = step3_export_table()
    # --- ION
    # export_arn = f"arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/{table_name}/export/01722628402072-5950e469"
    # --- DYNAMODB_JSON
    # export_arn = f"arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/{table_name}/export/01722628344835-adf2ac26"
    # step4_wait_export_complete(export_arn)
    # step5_check_export_results(export_arn)
