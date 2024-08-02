# -*- coding: utf-8 -*-

import typing as T
import enum
import json
import gzip
import dataclasses
from datetime import datetime, timezone

import botocore.exceptions

from .importer import amazon_ion, dynamodb_json


from .waiter import Waiter
from .utils import (
    parse_s3uri,
    T_ITEM,
    T_DATA,
)


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef
    from mypy_boto3_dynamodb.client import DynamoDBClient


class ImportStatusEnum(str, enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class ImportFormatEnum(str, enum.Enum):
    DYNAMODB_JSON = "DYNAMODB_JSON"
    ION = "ION"
    CSV = "CSV"


@dataclasses.dataclass
class ImportJob:
    """
    The DynamoDB import table job data model.

    Ref:

    - import_table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/import_table.html
    - describe_import: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_import.html
    - list_exports: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
    - How it works: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html
    """

    # fmt: off
    arn: str = dataclasses.field()
    status: str = dataclasses.field()
    table_arn: T.Optional[str] = dataclasses.field(default=None)
    table_id: T.Optional[str] = dataclasses.field(default=None)
    s3_bucket_owner: T.Optional[str] = dataclasses.field(default=None)
    s3_bucket: T.Optional[str] = dataclasses.field(default=None)
    s3_prefix: T.Optional[str] = dataclasses.field(default=None)
    error_count: T.Optional[int] = dataclasses.field(default=None)
    cloudwatch_log_group_arn: T.Optional[str] = dataclasses.field(default=None)
    input_format: T.Optional[str] = dataclasses.field(default=None)
    import_format_options: T.Optional[T.Dict[str, str]] = dataclasses.field(default=None)
    import_compression_type: T.Optional[str] = dataclasses.field(default=None)
    table_creation_parameters: T.Optional[T.Dict[str, str]] = dataclasses.field(default=None)
    start_time: T.Optional[datetime] = dataclasses.field(default=None)
    end_time: T.Optional[datetime] = dataclasses.field(default=None)
    processed_size_bytes: T.Optional[int] = dataclasses.field(default=None)
    processed_item_count: T.Optional[int] = dataclasses.field(default=None)
    imported_item_count: T.Optional[int] = dataclasses.field(default=None)
    failure_code: T.Optional[str] = dataclasses.field(default=None)
    failure_message: T.Optional[str] = dataclasses.field(default=None)
    # fmt: on

    def __post_init__(self):
        if self.s3_prefix is not None:
            if self.s3_prefix.endswith("/") is False:
                self.s3_prefix += "/"

    @classmethod
    def list_imports(
        cls,
        dynamodb_client: "DynamoDBClient",
        table_arn: str,
        page_size: int = 25,
        max_results: int = 1000,
        get_details: bool = False,
    ) -> T.List["ImportJob"]:
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_imports.html
        """
        imports = list()
        next_token = None
        while 1:
            kwargs = dict(
                TableArn=table_arn,
                MaxResults=page_size,
            )
            if next_token is not None:
                kwargs["NextToken"] = next_token
            res = dynamodb_client.list_imports(**kwargs)
            for dct in res.get("ImportSummaryList", []):
                import_arn = dct["ImportArn"]
                import_status = dct["ImportStatus"]
                if get_details:
                    import_job = cls.describe_import(dynamodb_client, import_arn)
                else:
                    import_job = cls(arn=import_arn, status=import_status)
                imports.append(import_job)
                if len(imports) >= max_results:
                    return imports

            _next_token = res.get("NextToken", "NOT_AVAILABLE")
            if _next_token == "NOT_AVAILABLE":
                break
            else:
                next_token = _next_token
        return imports

    @classmethod
    def from_import_description(cls, desc: dict):
        """
        :param desc: The import description dictionary from
            ``import_table``, ``describe_import`` or ``list_imports``.
        """
        return cls(
            arn=desc["ImportArn"],
            status=desc["ImportStatus"],
            table_arn=desc.get("TableArn"),
            table_id=desc.get("TableId"),
            s3_bucket_owner=desc.get("S3BucketSource", {}).get("S3BucketOwner"),
            s3_bucket=desc.get("S3BucketSource", {}).get("S3Bucket"),
            s3_prefix=desc.get("S3BucketSource", {}).get("S3KeyPrefix"),
            error_count=desc.get("ErrorCount"),
            cloudwatch_log_group_arn=desc.get("CloudWatchLogGroupArn"),
            input_format=desc.get("InputFormat"),
            import_format_options=desc.get("InputFormatOptions"),
            import_compression_type=desc.get("InputCompressionType"),
            table_creation_parameters=desc.get("TableCreationParameters"),
            start_time=desc.get("StartTime"),
            end_time=desc.get("EndTime"),
            processed_size_bytes=desc.get("ProcessedSizeBytes"),
            processed_item_count=desc.get("ProcessedItemCount"),
            imported_item_count=desc.get("ImportedItemCount"),
            failure_code=desc.get("FailureCode"),
            failure_message=desc.get("FailureMessage"),
        )

    @classmethod
    def describe_import(
        cls,
        dynamodb_client: "DynamoDBClient",
        import_arn: str,
    ) -> T.Optional["ImportJob"]:
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_import.html
        """
        try:
            res = dynamodb_client.describe_import(ImportArn=import_arn)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ImportNotFoundException":
                return None
            else:
                raise NotImplementedError
        desc = res["ImportTableDescription"]
        return cls.from_import_description(desc)

    def is_in_progress(self) -> bool:
        return self.status == ImportStatusEnum.IN_PROGRESS.value

    def is_completed(self) -> bool:
        return self.status == ImportStatusEnum.COMPLETED.value

    def is_cancelling(self) -> bool:
        return self.status == ImportStatusEnum.CANCELLING.value

    def is_cancelled(self) -> bool:
        return self.status == ImportStatusEnum.CANCELLED.value

    def is_failed(self) -> bool:
        return self.status == ImportStatusEnum.FAILED.value

    @classmethod
    def wait_until_complete(
        cls,
        dynamodb_client: "DynamoDBClient",
        import_arn: str,
        delays: int = 10,
        timeout: int = 900,
        verbose: bool = True,
    ) -> "ImportJob":
        """
        Wait until the DynamoDB import is completed.
        """
        for attempt, elapse in Waiter(
            delays=delays,
            timeout=timeout,
            instant=True,
            verbose=verbose,
        ):
            import_job = cls.describe_import(
                dynamodb_client=dynamodb_client,
                import_arn=import_arn,
            )
            if import_job.is_completed():
                return import_job
            elif import_job.status in [
                ImportStatusEnum.CANCELLING.value,
                ImportStatusEnum.CANCELLED.value,
                ImportStatusEnum.FAILED.value,
            ]:
                raise ValueError(f"Import failed: {import_job.failure_message}")
            else:
                pass


ITEM = "Item"


def write_amazon_ion(
    records: T.Iterable[T.Dict[str, T.Any]],
    s3uri: str,
    s3_client: "S3Client",
) -> "PutObjectOutputTypeDef":
    """
    Write records to S3 for DynamoDB import table.

    This function uses the Amazon Ion format and gzip compression.

    Ref:

    - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.Format.html#S3DataImport.Requesting.Formats.Ion

    .. note::

        the ``amazon.ion`` python library doesn't work well with the DynamoDB
        import table feature. The ``amazon.ion`` library will encode integer value
        like the standard ``json`` library. ``1`` to ``1``. However, the
        data for import table requires a "dot" after the integer, for example ``1.``.
        Looks like there's no way we can let the ``amazon.ion`` to add the "dot".
        If you want to construct dataset for DynamoDB import table, don't use
        ION, use DynamoDB JSON instead.

    :param records: The list of python dictionary representing the dynamodb item to write to S3.
    :param s3uri: The S3 URI to write the data to.
    :param s3_client: The S3 client to use.
    """
    raise NotImplementedError(
        "amazon.ion library doesn't work well with DynamoDB import table feature. "
        "Use DynamoDB JSON instead."
    )
    # -- this should be the right way to implement it, but it doesn't add the dot
    # -- so that it doesn't work
    lines = []
    for record in records:
        lines.append(amazon_ion.dumps({ITEM: record}, binary=False))
    lines.append("")
    content = gzip.compress("\n".join(lines).encode("utf-8"))

    # -- I also tried this hacky way, but it doesn't work either
    # s = amazon_ion.dumps([{ITEM: record} for record in records], binary=False)
    # content = gzip.compress((s + "\n").encode("utf-8"))

    bucket, key = parse_s3uri(s3uri=s3uri)
    return s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentType="text/plain",
        ContentEncoding="gzip",
    )


def write_dynamodb_json(
    records: T.Iterable[T.Dict[str, T.Any]],
    s3uri: str,
    s3_client: "S3Client",
) -> "PutObjectOutputTypeDef":
    """
    Write records to S3 for DynamoDB import table.

    This function uses the DynamoDB JSON format and gzip compression.

    Ref:

    - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.Format.html#S3DataImport.Requesting.Formats.DDBJson

    :param records: The list of python dictionary representing the dynamodb item to write to S3.
    :param s3uri: The S3 URI to write the data to.
    :param s3_client: The S3 client to use.
    """
    lines = []
    for record in records:
        lines.append(f'{{"{ITEM}": {dynamodb_json.dumps(record)}}}')
    lines.append("")
    content = gzip.compress("\n".join(lines).encode("utf-8"))
    bucket, key = parse_s3uri(s3uri=s3uri)
    return s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
