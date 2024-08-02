# -*- coding: utf-8 -*-

"""
DynamoDB export to S3 tool box.

Reference:

- DynamoDB data export to Amazon S3: how it works: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html

Usage:

.. code-block:: python

    from aws_dynamodb_export_to_s3 import Export
"""

import typing as T
import enum
import json
import gzip
import dataclasses
from datetime import datetime, timezone

import botocore.exceptions

try:
    import amazon.ion.simpleion as ion
    from amazon.ion.simple_types import IonPyDict

    has_amazon_ion = True
except ImportError:
    has_amazon_ion = False


from .waiter import Waiter
from .utils import (
    parse_s3uri,
    T_ITEM,
    T_DATA,
)


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_dynamodb.client import DynamoDBClient


def _parse_time(s: str) -> datetime:
    """
    Parse UTC datetime string into timezone aware datetime object.
    """
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


@dataclasses.dataclass
class ManifestSummary:
    """
    The ``manifest-summary.json`` file data model.
    """

    table_id: str
    table_arn: str
    s3_bucket: str
    s3_prefix: str
    item_count: int
    output_format: str
    start_time_str: str
    end_time_str: str
    export_time_str: str
    manifest_files_s3_key: str
    billed_size_bytes: int

    @property
    def start_time(self) -> datetime:
        return _parse_time(self.start_time_str)

    @property
    def end_time(self) -> datetime:
        return _parse_time(self.end_time_str)

    @property
    def export_time(self) -> datetime:
        return _parse_time(self.export_time_str)


@dataclasses.dataclass
class DataFile:
    """
    The ``s3://.../AWSDynamoDB/${timestamp}-${random_str]/data/${random_str}.json.gz``
    data file data model.

    :param item_count: number of item in this data file.
    :param md5: md5 hash of this data file.
    :param etag: AWS S3 etag.
    :param s3_bucket: S3 bucket name.
    :param s3_key: S3 key.
    """

    item_count: int
    md5: str
    etag: str
    s3_bucket: str
    s3_key: str

    def read_dynamodb_json(
        self,
        s3_client: "S3Client",
    ) -> T.List[T_ITEM]:
        """
        Read items from the DynamoDB JSON data file.

        Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html

        Example item::

            {
                'key1': {'S': '...'},
                'attr1': {'S': '...'},
                'attr2': {'N': '...'},
                ...
            },
        """
        res = s3_client.get_object(
            Bucket=self.s3_bucket,
            Key=self.s3_key,
        )
        lines = gzip.decompress(res["Body"].read()).decode("utf-8").splitlines()
        return [json.loads(line)["Item"] for line in lines]

    def read_amazon_ion(
        self,
        s3_client: "S3Client",
        converter: T.Callable[["IonPyDict"], T_DATA],
    ) -> T.List[T_DATA]:
        """
        Read items from the Amazon ION data file.

        :param s3_client: S3 client for reading data.
        :param converter: Convert Ion dict to Python dict.

        Ref: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html

        Example item::

            {
                'key1': ...,
                'attr1': ...,
                'attr2': ...,
                ...
            },
        """
        res = s3_client.get_object(
            Bucket=self.s3_bucket,
            Key=self.s3_key,
        )
        lines = gzip.decompress(res["Body"].read()).decode("utf-8").splitlines()
        rows = list()
        for line in lines:
            try:
                ion_dict = ion.loads(line)["Item"]
                row = converter(ion_dict)
                rows.append(row)
            except TypeError:
                pass
        return rows


class ExportStatusEnum(str, enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExportFormatEnum(str, enum.Enum):
    DYNAMODB_JSON = "DYNAMODB_JSON"
    ION = "ION"


@dataclasses.dataclass
class ExportJob:
    """
    The DynamoDB export table job data model.
    """

    arn: str = dataclasses.field()
    status: str = dataclasses.field()
    start_time: T.Optional[datetime] = dataclasses.field(default=None)
    end_time: T.Optional[datetime] = dataclasses.field(default=None)
    export_time: T.Optional[datetime] = dataclasses.field(default=None)
    table_arn: T.Optional[str] = dataclasses.field(default=None)
    table_id: T.Optional[str] = dataclasses.field(default=None)
    s3_bucket: T.Optional[str] = dataclasses.field(default=None)
    s3_prefix: T.Optional[str] = dataclasses.field(default=None)
    item_count: T.Optional[int] = dataclasses.field(default=None)
    export_format: T.Optional[str] = dataclasses.field(default=None)
    failure_code: T.Optional[str] = dataclasses.field(default=None)
    failure_message: T.Optional[str] = dataclasses.field(default=None)
    export_manifest: T.Optional[str] = dataclasses.field(default=None)

    def __post_init__(self):
        if self.s3_prefix is not None:
            if self.s3_prefix.endswith("/") is False:
                self.s3_prefix += "/"

    @classmethod
    def list_exports(
        cls,
        dynamodb_client: "DynamoDBClient",
        table_arn: str,
        page_size: int = 25,
        max_results: int = 1000,
        get_details: bool = False,
    ) -> T.List["ExportJob"]:
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
        """
        exports = list()
        next_token = None
        while 1:
            kwargs = dict(
                TableArn=table_arn,
                MaxResults=page_size,
            )
            if next_token is not None:
                kwargs["NextToken"] = next_token
            res = dynamodb_client.list_exports(**kwargs)
            for dct in res.get("ExportSummaries", []):
                export_arn = dct["ExportArn"]
                export_status = dct["ExportStatus"]
                if get_details:
                    export_job = cls.describe_export(dynamodb_client, export_arn)
                else:
                    export_job = cls(arn=export_arn, status=export_status)
                exports.append(export_job)
                if len(exports) >= max_results:
                    return exports

            _next_token = res.get("NextToken", "NOT_AVAILABLE")
            if _next_token == "NOT_AVAILABLE":
                break
            else:
                next_token = _next_token
        return exports

    @classmethod
    def from_export_description(cls, desc: dict):
        """
        :param desc: The export description dictionary from
            ``export_table_to_point_in_time``, ``describe_export`` or ``list_exports``.
        """
        return cls(
            arn=desc["ExportArn"],
            status=desc["ExportStatus"],
            start_time=desc.get("StartTime"),
            end_time=desc.get("EndTime"),
            export_time=desc.get("ExportTime"),
            table_arn=desc.get("TableArn"),
            table_id=desc.get("TableId"),
            s3_bucket=desc.get("S3Bucket"),
            s3_prefix=desc.get("S3Prefix"),
            item_count=desc.get("ItemCount"),
            export_format=desc.get("ExportFormat"),
            failure_code=desc.get("FailureCode"),
            failure_message=desc.get("FailureMessage"),
            export_manifest=desc.get("ExportManifest"),
        )

    @classmethod
    def describe_export(
        cls,
        dynamodb_client: "DynamoDBClient",
        export_arn: str,
    ) -> T.Optional["ExportJob"]:
        """
        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_export.html
        """
        try:
            res = dynamodb_client.describe_export(ExportArn=export_arn)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ExportNotFoundException":
                return None
            else:
                raise NotImplementedError

        desc = res["ExportDescription"]
        return cls.from_export_description(desc)

    def is_in_progress(self) -> bool:
        return self.status == ExportStatusEnum.IN_PROGRESS.value

    def is_completed(self) -> bool:
        return self.status == ExportStatusEnum.COMPLETED.value

    def is_failed(self) -> bool:
        return self.status == ExportStatusEnum.FAILED.value

    def is_dynamodb_json_format(self) -> bool:
        return self.export_format == ExportFormatEnum.DYNAMODB_JSON.value

    def is_ion_format(self) -> bool:
        return self.export_format == ExportFormatEnum.ION.value

    @property
    def export_short_id(self) -> str:
        """
        The short ID of the export, which is a compound of the export timestamp
        and random string. Example: ``1672531200000-a1b2c3d4``. 1672531200000
        is the timestamp of 2023-01-01 00:00:00
        """
        return self.arn.split("/")[-1]

    @property
    def s3uri_export(self) -> str:
        """
        The S3 folder you specified when you call the
        ``dynamodb_client.export_table_to_point_in_time(...)`` API.

        Example: s3://bucket/prefix/
        """
        return f"s3://{self.s3_bucket}/{self.s3_prefix}"

    @property
    def _s3uri_export_root(self) -> str:
        return f"{self.s3uri_export}AWSDynamoDB/{self.export_short_id}/"

    @property
    def s3uri_export_data(self) -> str:
        """
        Where the export data files are stored.

        Example: s3://bucket/prefix/AWSDynamoDB/1672531200000-a1b2c3d4/data/
        """
        return f"{self._s3uri_export_root}data/"

    @property
    def s3uri_export_manifest_files(self) -> str:
        """
        The S3 location of the manifest files.

        Example: s3://bucket/prefix/AWSDynamoDB/1672531200000-a1b2c3d4/manifest-files.json
        """
        return f"{self._s3uri_export_root}manifest-files.json"

    @property
    def s3uri_export_manifest_summary(self) -> str:
        """
        The S3 location of the manifest summary file.

        Example: s3://bucket/prefix/AWSDynamoDB/1672531200000-a1b2c3d4/manifest-summary.json
        """
        return f"{self._s3uri_export_root}manifest-summary.json"

    def get_details(self, dynamodb_client: "DynamoDBClient"):
        """
        Get the details of the DynamoDB export, refresh it's attributes values.
        """
        export = self.describe_export(
            dynamodb_client=dynamodb_client, export_arn=self.arn
        )
        for field in dataclasses.fields(self.__class__):
            setattr(self, field.name, getattr(export, field.name))
        self.__post_init__()

    def _ensure_details(self, dynamodb_client: "DynamoDBClient"):
        """
        Ensure that the details of the DynamoDB export are available.
        If not, call ``get_details`` to refresh the attributes values.
        """
        if self.s3_bucket is None:
            self.get_details(dynamodb_client=dynamodb_client)

    def get_manifest_summary(
        self,
        dynamodb_client: "DynamoDBClient",
        s3_client: "S3Client",
    ) -> ManifestSummary:
        """
        Get the manifest summary of the DynamoDB export.
        """
        self._ensure_details(dynamodb_client=dynamodb_client)
        bucket, key = parse_s3uri(self.s3uri_export_manifest_summary)
        res = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        data = json.loads(res["Body"].read().decode("utf-8"))
        return ManifestSummary(
            table_id=data["tableId"],
            table_arn=data["tableArn"],
            s3_bucket=data["s3Bucket"],
            s3_prefix=data["s3Prefix"],
            item_count=data["itemCount"],
            output_format=data["outputFormat"],
            start_time_str=data["startTime"],
            end_time_str=data["endTime"],
            export_time_str=data["exportTime"],
            manifest_files_s3_key=data["manifestFilesS3Key"],
            billed_size_bytes=data["billedSizeBytes"],
        )

    def get_data_files(
        self,
        dynamodb_client: "DynamoDBClient",
        s3_client: "S3Client",
    ) -> T.List[DataFile]:
        """
        Get the list of data files of the DynamoDB export.
        """
        self._ensure_details(dynamodb_client=dynamodb_client)
        bucket, key = parse_s3uri(self.s3uri_export_manifest_files)
        res = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        lines = res["Body"].read().decode("utf-8").splitlines()
        data_file_list = list()
        for line in lines:
            data = json.loads(line)
            data_file_list.append(
                DataFile(
                    item_count=data["itemCount"],
                    md5=data["md5Checksum"],
                    etag=data["etag"],
                    s3_bucket=bucket,
                    s3_key=data["dataFileS3Key"],
                )
            )
        return data_file_list

    def read_dynamodb_json(
        self,
        dynamodb_client: "DynamoDBClient",
        s3_client: "S3Client",
    ) -> T.Iterable[T_ITEM]:
        """
        Read the items of the DynamoDB export. This is a generator function.
        """
        data_file_list = self.get_data_files(
            dynamodb_client=dynamodb_client,
            s3_client=s3_client,
        )
        for data_file in data_file_list:
            for item in data_file.read_dynamodb_json(s3_client=s3_client):
                yield item

    def read_amazon_ion(
        self,
        dynamodb_client: "DynamoDBClient",
        s3_client: "S3Client",
        converter: T.Callable[["IonPyDict"], T_DATA],
    ) -> T.Iterable[T_ITEM]:
        """
        Read the items of the DynamoDB export. This is a generator function.
        """
        data_file_list = self.get_data_files(
            dynamodb_client=dynamodb_client,
            s3_client=s3_client,
        )
        for data_file in data_file_list:
            for item in data_file.read_amazon_ion(
                s3_client=s3_client,
                converter=converter,
            ):
                yield item

    @classmethod
    def export_table_to_point_in_time(
        cls,
        dynamodb_client: "DynamoDBClient",
        table_arn: str,
        s3_bucket: str,
        s3_prefix: T.Optional[str] = None,
        export_time: T.Optional[datetime] = None,
        s3_bucket_owner: T.Optional[datetime] = None,
        s3_sse_algorithm: T.Optional[datetime] = None,
        s3_sse_kms_key_id: T.Optional[datetime] = None,
        export_format: str = ExportFormatEnum.DYNAMODB_JSON.value,
        client_token: T.Optional[str] = None,
    ):
        """
        Export DynamoDB to point-in-time, and return the export object.

        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/export_table_to_point_in_time.html
        """
        kwargs = dict(
            TableArn=table_arn,
            S3Bucket=s3_bucket,
            S3Prefix=s3_prefix,
            ExportTime=export_time,
            S3BucketOwner=s3_bucket_owner,
            S3SseAlgorithm=s3_sse_algorithm,
            S3SseKmsKeyId=s3_sse_kms_key_id,
            ExportFormat=export_format,
            ClientToken=client_token,
        )
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        res = dynamodb_client.export_table_to_point_in_time(**kwargs)
        desc = res["ExportDescription"]
        return cls.from_export_description(desc)

    @classmethod
    def wait_until_complete(
        cls,
        dynamodb_client: "DynamoDBClient",
        export_arn: str,
        delays: int = 10,
        timeout: int = 900,
        verbose: bool = True,
    ) -> "ExportJob":
        """
        Wait until the DynamoDB export is completed.
        """
        for attempt, elapse in Waiter(
            delays=delays,
            timeout=timeout,
            instant=True,
            verbose=verbose,
        ):
            export = cls.describe_export(
                dynamodb_client=dynamodb_client,
                export_arn=export_arn,
            )
            if export.is_completed():
                return export
            elif export.is_failed():
                raise ValueError(f"Export failed: {export.failure_message}")
            else:
                pass
