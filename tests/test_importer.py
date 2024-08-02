# -*- coding: utf-8 -*-


def test():
    data = {"id": 1, "name": "Alice"}
    try:
        from aws_dynamodb_io.importer import amazon_ion

        amazon_ion.dumps(data)
    except ImportError:
        pass

    try:
        from aws_dynamodb_io.importer import dynamodb_json

        dynamodb_json.dumps(data)
    except ImportError:
        pass


if __name__ == "__main__":
    from aws_dynamodb_io.tests import run_cov_test

    run_cov_test(__file__, "aws_dynamodb_io.importer", preview=False)
