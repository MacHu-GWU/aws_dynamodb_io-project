# -*- coding: utf-8 -*-

import amazon.ion.simpleion as ion
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
print(ion.dumps(py_data))


print("--- Dump one record to file ---")
dir_here = Path(__file__).absolute().parent
path = dir_here / "record.ion"
with path.open("wb") as f:
    ion.dump(py_data, f)


print("--- Dump one record to bytes ---")
print(ion.dumps([py_data]))


print("--- Dump one record to file ---")
dir_here = Path(__file__).absolute().parent
path = dir_here / "dataset.ion"
with path.open("wb") as f:
    ion.dump([py_data], f)

dir_here = Path(__file__).absolute().parent
path = dir_here / "dataset.ion"
with path.open("wb") as f:
    ion.dump([py_data], f)
