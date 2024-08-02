# -*- coding: utf-8 -*-

import typing as T


def parse_s3uri(s3uri: str) -> T.Tuple[str, str]:
    """
    Parse S3 URI into bucket and key.

    Example::

        >>> parse_s3uri("s3://my-bucket/folder/file.txt")
        ('my-bucket', 'folder/file.txt')
    """
    parts = s3uri.split("/", 3)
    bucket = parts[2]
    key = parts[3]
    return bucket, key


# Represent a DynamoDB JSON data
# {"key": {"S": "encoded_value"}}
T_ITEM = T.Dict[str, T.Dict[str, T.Any]]

# Represent a native Python dictionary data
T_DATA = T.Dict[str, T.Any]
