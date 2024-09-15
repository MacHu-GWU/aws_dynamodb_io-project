.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.1.6 (2024-09-15)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add support to do incremental export in ``ExportJob.export_table_to_point_in_time`` method.
- Add the following Public APIs:
    - ``aws_dynamodb_io.api.ExportTypeEnum``


0.1.5 (2024-09-12)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add the ``ExportJob.from_s3_dir`` method to create an ``ExportJob`` object from an S3 directory directly.
- Add the following Public APIs:
    - ``aws_dynamodb_io.api.ExportJob.from_s3_dir``


0.1.4 (2024-08-16)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- Add ``client_token``, ``s3_sse_algorithm``, ``s3_sse_kms_key_id``, ``billed_size_bytes``, ``export_type``, ``incremental_export_specification`` attributes to ``ExportJob``
- Add ``client_token`` attribute to ``ImportJob``.


0.1.3 (2024-08-05)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- Add ``export_arn`` and ``export_format`` attribute to ``aws_dynamodb_io.api.DataFile`` class.


0.1.2 (2024-08-02)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add the following Public APIs:
    - ``aws_dynamodb_io.api.parse_s3uri``
- Rename ``aws_dynamodb_io.api.T_ITEM`` to ``aws_dynamodb_io.api.T_DNAMODB_JSON``
- ``aws_dynamodb_io.api.DataFile.read_amazon_ion`` and ``aws_dynamodb_io.api.ExportJob.read_amazon_ion`` method now returns a list of ``amazon.ion.simple_types.IonPyDict`` object. We leave the decision to the user to convert the IonPyDict object to a Python dictionary or any other data structure as needed.

**Bugfixes**

- Remove unnecessary ``dynamodb-json`` dependency from the ``requirements.txt`` file.


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
