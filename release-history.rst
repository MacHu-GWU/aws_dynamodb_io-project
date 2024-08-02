.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.1.1 (2024-08-01)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release
- Add the following public APIs:
    - ``aws_dynamodb_io.api.T_ITEM``
    - ``aws_dynamodb_io.api.T_DATA``
    - ``aws_dynamodb_io.api.ImportStatusEnum``
    - ``aws_dynamodb_io.api.ImportFormatEnum``
    - ``aws_dynamodb_io.api.ImportJob``
    - ``aws_dynamodb_io.api.ImportJob.list_imports``
    - ``aws_dynamodb_io.api.ImportJob.from_import_description``
    - ``aws_dynamodb_io.api.ImportJob.describe_import``
    - ``aws_dynamodb_io.api.ImportJob.is_in_progress``
    - ``aws_dynamodb_io.api.ImportJob.is_completed``
    - ``aws_dynamodb_io.api.ImportJob.is_cancelling``
    - ``aws_dynamodb_io.api.ImportJob.is_cancelled``
    - ``aws_dynamodb_io.api.ImportJob.is_failed``
    - ``aws_dynamodb_io.api.ImportJob.wait_until_complete``
    - ``aws_dynamodb_io.api.write_amazon_ion``
    - ``aws_dynamodb_io.api.write_dynamodb_json``
    - ``aws_dynamodb_io.api.ManifestSummary``
    - ``aws_dynamodb_io.api.parse_s3uri``
    - ``aws_dynamodb_io.api.ExportStatusEnum``
    - ``aws_dynamodb_io.api.ExportFormatEnum``
    - ``aws_dynamodb_io.api.DataFile``
    - ``aws_dynamodb_io.api.DataFile.read_dynamodb_json``
    - ``aws_dynamodb_io.api.DataFile.read_amazon_ion``
    - ``aws_dynamodb_io.api.ExportJob``
    - ``aws_dynamodb_io.api.ExportJob.list_exports``
    - ``aws_dynamodb_io.api.ExportJob.describe_export``
    - ``aws_dynamodb_io.api.ExportJob.is_in_progress``
    - ``aws_dynamodb_io.api.ExportJob.is_completed``
    - ``aws_dynamodb_io.api.ExportJob.is_failed``
    - ``aws_dynamodb_io.api.ExportJob.is_dynamodb_json_format``
    - ``aws_dynamodb_io.api.ExportJob.is_ion_format``
    - ``aws_dynamodb_io.api.ExportJob.export_short_id``
    - ``aws_dynamodb_io.api.ExportJob.s3uri_export``
    - ``aws_dynamodb_io.api.ExportJob.s3uri_export_data``
    - ``aws_dynamodb_io.api.ExportJob.s3uri_export_manifest_files``
    - ``aws_dynamodb_io.api.ExportJob.s3uri_export_manifest_summary``
    - ``aws_dynamodb_io.api.ExportJob.get_details``
    - ``aws_dynamodb_io.api.ExportJob.get_manifest_summary``
    - ``aws_dynamodb_io.api.ExportJob.get_data_files``
    - ``aws_dynamodb_io.api.ExportJob.read_dynamodb_json``
    - ``aws_dynamodb_io.api.ExportJob.read_amazon_ion``
    - ``aws_dynamodb_io.api.ExportJob.export_table_to_point_in_time``
    - ``aws_dynamodb_io.api.ExportJob.wait_until_complete``
