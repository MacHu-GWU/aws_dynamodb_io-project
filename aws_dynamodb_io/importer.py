# -*- coding: utf-8 -*-


amazon_ion_error = ImportError(
    "You don't have amazon-ion installed. "
    "Please install it to use this feature. "
    "See https://pypi.org/project/amazon.ion/ for more details"
)


class FakeAmazonIon:  # pragma: no cover
    def dumps(self, *args, **kwargs):
        raise amazon_ion_error


dynamodb_json_error = ImportError(
    "You don't have dynamodb-json installed. "
    "Please install it to use this feature. "
    "See https://pypi.org/project/dynamodb-json/ for more details"
)


class FakeDynamoDBJson:  # pragma: no cover
    def dumps(self, *args, **kwargs):
        raise dynamodb_json_error


try:  # pragma: no cover
    import amazon.ion.simpleion as amazon_ion
    from amazon.ion.simple_types import IonPyDict

    has_amazon_ion = True
except ImportError:  # pragma: no cover
    amazon_ion = FakeAmazonIon()
    has_amazon_ion = False

try:  # pragma: no cover
    from dynamodb_json import json_util as dynamodb_json

    has_dynamodb_json = True
except ImportError:  # pragma: no cover
    dynamodb_json = FakeDynamoDBJson()
    has_dynamodb_json = False
