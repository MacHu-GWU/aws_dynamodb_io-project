# -*- coding: utf-8 -*-

from dynamodb_json import json_util
from pathlib import Path

py_data = {
    "id": 1,
    "name": "Alice",
    "bio": {
        "dob": "1990-01-01",
        "address": "123 Main St.",
    },
    "relationships": [
        {"name": "Bob", "relation": "friend"},
        {"name": "Charlie", "relation": "father"},
    ],
}

print("--- Dump one record to bytes ---")
print(json_util.dumps(py_data))
