# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from aws_dynamodb_io.tests import run_cov_test

    run_cov_test(__file__, "aws_dynamodb_io", is_folder=True, preview=False)
