# -*- coding: utf-8 -*-

"""
DynamoDB export to S3 tool box.

Reference:

- DynamoDB data export to Amazon S3: how it works: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html
"""

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
    split_s3_uri,
    T_DNAMODB_JSON,
    T_DATA,
)


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from amazon.ion.simple_types import IonPyDict


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

    version: str
    export_arn: str
    table_id: str
    table_arn: str
    s3_bucket: str
    s3_prefix: str
    s3_sse_algorithm: str
    s3_sse_kms_key_id: str
    item_count: int
    output_format: str
    output_view: T.Optional[str]
    export_from_time_str: T.Optional[str]
    export_to_time_str: T.Optional[str]
    start_time_str: str
    end_time_str: str
    export_time_str: T.Optional[str]
    manifest_files_s3_key: str
    billed_size_bytes: int
    export_type: str

    @property
    def start_time(self) -> datetime:
        return _parse_time(self.start_time_str)

    @property
    def end_time(self) -> datetime:
        return _parse_time(self.end_time_str)

    @property
    def export_time(self) -> datetime:
        return _parse_time(self.export_time_str)

    @property
    def export_from_time(self) -> datetime:
        return _parse_time(self.export_from_time_str)

    @property
    def export_to_time(self) -> datetime:
        return _parse_time(self.export_to_time_str)

    def is_full_export(self) -> bool:
        return self.export_type == ExportTypeEnum.FULL_EXPORT.value

    def is_incremental_export(self) -> bool:
        return self.export_type == ExportTypeEnum.INCREMENTAL_EXPORT.value

    @classmethod
    def from_dict(cls, data: T_DATA):
        return cls(
            version=data["version"],
            export_arn=data["exportArn"],
            table_id=data["tableId"],
            table_arn=data["tableArn"],
            s3_bucket=data["s3Bucket"],
            s3_prefix=data["s3Prefix"],
            s3_sse_algorithm=data["s3SseAlgorithm"],
            s3_sse_kms_key_id=data.get("s3SseKmsKeyId"),
            item_count=data["itemCount"],
            output_format=data["outputFormat"],
            output_view=data.get("outputView"),
            export_from_time_str=data.get("exportFromTime"),
            export_to_time_str=data.get("exportToTime"),
            start_time_str=data["startTime"],
            end_time_str=data["endTime"],
            export_time_str=data.get("exportTime"),
            manifest_files_s3_key=data["manifestFilesS3Key"],
            billed_size_bytes=data["billedSizeBytes"],
            export_type=data["exportType"],
        )

    def to_dict(self) -> T_DATA:
        """
        Convert the object to a dictionary.
        """
        if self.is_full_export():
            return dict(
                version=self.version,
                exportArn=self.export_arn,
                tableId=self.table_id,
                tableArn=self.table_arn,
                s3Bucket=self.s3_bucket,
                s3Prefix=self.s3_prefix,
                s3SseAlgorithm=self.s3_sse_algorithm,
                s3SseKmsKeyId=self.s3_sse_kms_key_id,
                itemCount=self.item_count,
                outputFormat=self.output_format,
                startTime=self.start_time_str,
                endTime=self.end_time_str,
                exportTime=self.export_time_str,
                manifestFilesS3Key=self.manifest_files_s3_key,
                billedSizeBytes=self.billed_size_bytes,
                exportType=self.export_type,
            )
        elif self.is_incremental_export():
            return dict(
                version=self.version,
                exportArn=self.export_arn,
                tableId=self.table_id,
                tableArn=self.table_arn,
                s3Bucket=self.s3_bucket,
                s3Prefix=self.s3_prefix,
                s3SseAlgorithm=self.s3_sse_algorithm,
                s3SseKmsKeyId=self.s3_sse_kms_key_id,
                itemCount=self.item_count,
                outputFormat=self.output_format,
                outputView=self.output_view,
                exportFromTime=self.export_from_time_str,
                exportToTime=self.export_to_time_str,
                startTime=self.start_time_str,
                endTime=self.end_time_str,
                manifestFilesS3Key=self.manifest_files_s3_key,
                billedSizeBytes=self.billed_size_bytes,
                exportType=self.export_type,
            )
        else:  # pragma: no cover
            raise NotImplementedError


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
    export_arn: str
    export_format: str

    @property
    def s3_uri(self) -> str:
        """
        The S3 URI of the data file.
        """
        return f"s3://{self.s3_bucket}/{self.s3_key}"

    def read_dynamodb_json(
        self,
        s3_client: "S3Client",
    ) -> T.List[T_DNAMODB_JSON]:
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
        ion_loads_kwargs: T.Dict[str, T.Any] = None,
    ) -> T.List["IonPyDict"]:
        """
        Read items from the Amazon ION data file.

        :param s3_client: S3 client for reading data.

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
        ion_dict_list = list()
        if ion_loads_kwargs is None:
            # use BARE to load native Python object
            ion_loads_kwargs = dict(value_model=amazon_ion.IonPyValueModel.MAY_BE_BARE)
        for line in lines:
            try:
                ion_dict = amazon_ion.loads(line, **ion_loads_kwargs)["Item"]
                ion_dict_list.append(ion_dict)
            except TypeError:
                pass
        return ion_dict_list


class ExportStatusEnum(str, enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExportFormatEnum(str, enum.Enum):
    DYNAMODB_JSON = "DYNAMODB_JSON"
    ION = "ION"


class ExportTypeEnum(str, enum.Enum):
    FULL_EXPORT = "FULL_EXPORT"
    INCREMENTAL_EXPORT = "INCREMENTAL_EXPORT"


@dataclasses.dataclass
class ExportJob:
    """
    The DynamoDB export table job data model.

    Ref:

    - export_table_to_point_in_time: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/export_table_to_point_in_time.html
    - describe_export: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_export.html
    - list_exports: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_exports.html
    - How it works: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html
    """

    arn: str = dataclasses.field()
    status: str = dataclasses.field()
    start_time: T.Optional[datetime] = dataclasses.field(default=None)
    end_time: T.Optional[datetime] = dataclasses.field(default=None)
    export_time: T.Optional[datetime] = dataclasses.field(default=None)
    table_arn: T.Optional[str] = dataclasses.field(default=None)
    table_id: T.Optional[str] = dataclasses.field(default=None)
    client_token: T.Optional[str] = dataclasses.field(default=None)
    s3_bucket: T.Optional[str] = dataclasses.field(default=None)
    s3_prefix: T.Optional[str] = dataclasses.field(default=None)
    s3_sse_algorithm: T.Optional[str] = dataclasses.field(default=None)
    s3_sse_kms_key_id: T.Optional[str] = dataclasses.field(default=None)
    billed_size_bytes: T.Optional[int] = dataclasses.field(default=None)
    item_count: T.Optional[int] = dataclasses.field(default=None)
    export_format: T.Optional[str] = dataclasses.field(default=None)
    failure_code: T.Optional[str] = dataclasses.field(default=None)
    failure_message: T.Optional[str] = dataclasses.field(default=None)
    export_manifest: T.Optional[str] = dataclasses.field(default=None)
    export_type: T.Optional[str] = dataclasses.field(default=None)
    incremental_export_specification: T.Optional[dict] = dataclasses.field(default=None)

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
            client_token=desc.get("ClientToken"),
            s3_bucket=desc.get("S3Bucket"),
            s3_prefix=desc.get("S3Prefix"),
            s3_sse_algorithm=desc.get("S3SseAlgorithm"),
            s3_sse_kms_key_id=desc.get("S3SseKmsKeyId"),
            billed_size_bytes=desc.get("BilledSizeBytes"),
            item_count=desc.get("ItemCount"),
            export_format=desc.get("ExportFormat"),
            failure_code=desc.get("FailureCode"),
            failure_message=desc.get("FailureMessage"),
            export_manifest=desc.get("ExportManifest"),
            export_type=desc.get("ExportType"),
            incremental_export_specification=desc.get("IncrementalExportSpecification"),
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
        bucket, key = split_s3_uri(self.s3uri_export_manifest_summary)
        res = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        data = json.loads(res["Body"].read().decode("utf-8"))
        return ManifestSummary.from_dict(data)

    def get_data_files(
        self,
        dynamodb_client: "DynamoDBClient",
        s3_client: "S3Client",
    ) -> T.List[DataFile]:
        """
        Get the list of data files of the DynamoDB export.
        """
        self._ensure_details(dynamodb_client=dynamodb_client)
        bucket, key = split_s3_uri(self.s3uri_export_manifest_files)
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
                    export_arn=self.arn,
                    export_format=self.export_format,
                )
            )
        return data_file_list

    def read_dynamodb_json(
        self,
        dynamodb_client: "DynamoDBClient",
        s3_client: "S3Client",
    ) -> T.Iterable[T_DNAMODB_JSON]:
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
    ) -> T.Iterable["IonPyDict"]:
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
        export_type: str = ExportTypeEnum.FULL_EXPORT.value,
        client_token: T.Optional[str] = None,
        incremental_export_specification: T.Optional[dict] = None,
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
            ExportType=export_type,
            ClientToken=client_token,
            IncrementalExportSpecification=incremental_export_specification,
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

    @classmethod
    def from_s3_dir(
        cls,
        s3_client: "S3Client",
        bucket: str,
        prefix: str,
    ):  # pragma: no cover
        """
        Unlike describe_export reading from DynamoDB API, it directly reads the
        export metadata from the S3 folder of a completed export job. The DynamoDB
        export is only available for 35 days after the export is completed. After
        that, you can use this method to read the export from S3 directly.

        :param s3_client: The boto3 S3 client
        :param bucket: The S3 bucket
        :param prefix: It should have a manifest-summary.json file in it.
            Example: "my-dynamodb-export/AWSDynamoDB/01725162280092-940349cc/
        """
        if prefix.endswith("/") is False:
            prefix = prefix + "/"
        # check if it a valid export folder
        s3_client.head_object(Bucket=bucket, Key=f"{prefix}manifest-files.md5")
        s3_client.head_object(Bucket=bucket, Key=f"{prefix}manifest-files.json")
        s3_client.head_object(Bucket=bucket, Key=f"{prefix}manifest-summary.md5")
        res = s3_client.get_object(Bucket=bucket, Key=f"{prefix}manifest-summary.json")
        data = json.loads(res["Body"].read().decode("utf-8"))
        return cls(
            arn=data["exportArn"],
            status=ExportStatusEnum.COMPLETED.value,
            start_time=datetime.strptime(
                data.get("startTime"), "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            end_time=datetime.strptime(data.get("endTime"), "%Y-%m-%dT%H:%M:%S.%fZ"),
            export_time=datetime.strptime(
                data.get("exportTime"), "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            table_arn=data.get("tableArn"),
            table_id=data.get("tableId"),
            s3_bucket=data.get("s3Bucket"),
            s3_prefix=data.get("s3Prefix"),
            s3_sse_algorithm=data.get("s3SseAlgorithm"),
            s3_sse_kms_key_id=data.get("s3SseKmsKeyId"),
            billed_size_bytes=data.get("billedSizeBytes"),
            item_count=data.get("itemCount"),
            export_format=data.get("outputFormat"),
            export_manifest=data.get("manifestFilesS3Key"),
            export_type=data.get("exportType"),
            incremental_export_specification=data.get("IncrementalExportSpecification"),
        )
