# -*- coding: utf-8 -*-

from aws_dynamodb_io import api


def test():
    _ = api
    _ = api.T_ITEM
    _ = api.T_DATA
    _ = api.ImportStatusEnum
    _ = api.ImportFormatEnum
    _ = api.ImportJob
    _ = api.ImportJob.list_imports
    _ = api.ImportJob.from_import_description
    _ = api.ImportJob.describe_import
    _ = api.ImportJob.is_in_progress
    _ = api.ImportJob.is_completed
    _ = api.ImportJob.is_cancelling
    _ = api.ImportJob.is_cancelled
    _ = api.ImportJob.is_failed
    _ = api.ImportJob.wait_until_complete
    _ = api.write_amazon_ion
    _ = api.write_dynamodb_json
    _ = api.ManifestSummary
    _ = api.parse_s3uri
    _ = api.ExportStatusEnum
    _ = api.ExportFormatEnum
    _ = api.DataFile
    _ = api.DataFile.read_dynamodb_json
    _ = api.DataFile.read_amazon_ion
    _ = api.ExportJob
    _ = api.ExportJob.list_exports
    _ = api.ExportJob.describe_export
    _ = api.ExportJob.is_in_progress
    _ = api.ExportJob.is_completed
    _ = api.ExportJob.is_failed
    _ = api.ExportJob.is_dynamodb_json_format
    _ = api.ExportJob.is_ion_format
    _ = api.ExportJob.export_short_id
    _ = api.ExportJob.s3uri_export
    _ = api.ExportJob.s3uri_export_data
    _ = api.ExportJob.s3uri_export_manifest_files
    _ = api.ExportJob.s3uri_export_manifest_summary
    _ = api.ExportJob.get_details
    _ = api.ExportJob.get_manifest_summary
    _ = api.ExportJob.get_data_files
    _ = api.ExportJob.read_dynamodb_json
    _ = api.ExportJob.read_amazon_ion
    _ = api.ExportJob.export_table_to_point_in_time
    _ = api.ExportJob.wait_until_complete


if __name__ == "__main__":
    from aws_dynamodb_io.tests import run_cov_test

    run_cov_test(__file__, "aws_dynamodb_io.api", preview=False)
