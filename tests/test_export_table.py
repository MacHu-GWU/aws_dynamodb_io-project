# -*- coding: utf-8 -*-

"""
Reference:

- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html
"""

from aws_dynamodb_io.export_table import ManifestSummary, DataFile


def test_():
    full_export_data = {
        "version": "2020-06-30",
        "exportArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog/export/01234567890123-a1b2c3d4",
        "startTime": "2020-11-04T07:28:34.028Z",
        "endTime": "2020-11-04T07:33:43.897Z",
        "tableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog",
        "tableId": "12345a12-abcd-123a-ab12-1234abc12345",
        "exportTime": "2020-11-04T07:28:34.028Z",
        "s3Bucket": "ddb-productcatalog-export",
        "s3Prefix": "2020-Nov",
        "s3SseAlgorithm": "AES256",
        "s3SseKmsKeyId": None,
        "manifestFilesS3Key": "AWSDynamoDB/01693685827463-2d8752fd/manifest-files.json",
        "billedSizeBytes": 0,
        "itemCount": 8,
        "outputFormat": "DYNAMODB_JSON",
        "exportType": "FULL_EXPORT",
    }
    full_export_summary = ManifestSummary.from_dict(full_export_data)
    full_export_data1 = full_export_summary.to_dict()
    assert full_export_data1 == full_export_data
    assert ManifestSummary.from_dict(full_export_data1) == full_export_summary

    incr_export_data = {
        "version": "2023-08-01",
        "exportArn": "arn:aws:dynamodb:us-east-1:599882009758:table/export-test/export/01695097218000-d6299cbd",
        "startTime": "2023-09-19T04:20:18.000Z",
        "endTime": "2023-09-19T04:40:24.780Z",
        "tableArn": "arn:aws:dynamodb:us-east-1:599882009758:table/export-test",
        "tableId": "b116b490-6460-4d4a-9a6b-5d360abf4fb3",
        "exportFromTime": "2023-09-18T17:00:00.000Z",
        "exportToTime": "2023-09-19T04:00:00.000Z",
        "s3Bucket": "jason-exports",
        "s3Prefix": "20230919-prefix",
        "s3SseAlgorithm": "AES256",
        "s3SseKmsKeyId": None,
        "manifestFilesS3Key": "20230919-prefix/AWSDynamoDB/01693685934212-ac809da5/manifest-files.json",
        "billedSizeBytes": 20901239349,
        "itemCount": 169928274,
        "outputFormat": "DYNAMODB_JSON",
        "outputView": "NEW_AND_OLD_IMAGES",
        "exportType": "INCREMENTAL_EXPORT",
    }
    incr_export_summary = ManifestSummary.from_dict(incr_export_data)
    incr_export_data1 = incr_export_summary.to_dict()
    assert incr_export_data1 == incr_export_data
    assert ManifestSummary.from_dict(incr_export_data1) == incr_export_summary


if __name__ == "__main__":
    from aws_dynamodb_io.tests import run_cov_test

    run_cov_test(__file__, "aws_dynamodb_io.export_table", preview=False)
