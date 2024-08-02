# -*- coding: utf-8 -*-

import aws_dynamodb_io.api as dio
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager


# ------------------------------------------------------------------------------
# **Update the following variables**
# ------------------------------------------------------------------------------
aws_profile = "bmt_app_dev_us_east_1"
import_id = "import-1"
import_client_token = "2024-08-02 03:42 PM"
table_name = "aws_dyanmodb_io-import_table_example-person"
# input_format = "ION"  # ION | DYNAMODB_JSON
input_format = "DYNAMODB_JSON"  # ION | DYNAMODB_JSON

# ------------------------------------------------------------------------------
# Don't change the code below
# ------------------------------------------------------------------------------
bsm = BotoSesManager(profile_name=aws_profile)
aws_region = bsm.aws_region
aws_account_id = bsm.aws_account_id
context.attach_boto_session(boto_ses=bsm.boto_ses)

s3dir_root = S3Path(
    f"s3://{bsm.aws_account_alias}-{aws_region}-data/projects/aws_dynamodb_io/poc/"
)
s3dir_import = (s3dir_root / "imports" / import_id).to_dir()
table_url = (
    f"https://{aws_region}.console.aws.amazon.com/dynamodbv2"
    f"/home?region={aws_region}#table?name={table_name}"
)


def step1_prepare_data():
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

    print(f"import table from {s3dir_import.uri}")
    print(f"preview import data: {s3dir_import.console_url}")

    s3dir_import.delete()

    if input_format == "ION":
        s3path = s3dir_import / "1.ion.gz"
        dio.write_amazon_ion(
            records=records,
            s3uri=s3path.uri,
            s3_client=bsm.s3_client,
        )
    elif input_format == "DYNAMODB_JSON":
        s3path = s3dir_import / "1.json.gz"
        dio.write_dynamodb_json(
            records=records,
            s3uri=s3path.uri,
            s3_client=bsm.s3_client,
        )
    else:
        raise NotImplementedError


def step2_import_table():
    if input_format == "ION":
        InputFormat = dio.ImportFormatEnum.ION.value
    elif input_format == "DYNAMODB_JSON":
        InputFormat = dio.ImportFormatEnum.DYNAMODB_JSON.value
    else:
        raise NotImplementedError
    res = bsm.dynamodb_client.import_table(
        ClientToken=import_client_token,
        S3BucketSource=dict(
            S3Bucket=s3dir_import.bucket,
            S3KeyPrefix=s3dir_import.key,
        ),
        InputFormat=InputFormat,
        InputCompressionType="GZIP",
        TableCreationParameters=dict(
            TableName=table_name,
            AttributeDefinitions=[
                dict(AttributeName="id", AttributeType="N"),
            ],
            KeySchema=[
                dict(AttributeName="id", KeyType="HASH"),
            ],
            BillingMode="PAY_PER_REQUEST",
        ),
    )
    import_arn = res["ImportTableDescription"]["ImportArn"]
    print(f"{import_arn = }")
    return import_arn


def step3_wait_import_complete(import_arn: str):
    print(f"preview dynamodb table: {table_url}")
    dio.ImportJob.wait_until_complete(
        dynamodb_client=bsm.dynamodb_client,
        import_arn=import_arn,
    )


if __name__ == "__main__":
    """
    Execute steps one by one.
    """
    # step1_prepare_data()
    # import_arn = step2_import_table()
    # import_arn = f"arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/{table_name}/import/01722627754385-67f5dfbe"
    # step3_wait_import_complete(import_arn)
