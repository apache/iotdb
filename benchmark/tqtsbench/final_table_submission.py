# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Gold-independent final-table-v1 serialization; never recovers tool/prose answers."""
import json
import math


def serialize_final_table(final):
    table = final.get("final_table") if isinstance(final, dict) else None
    if not isinstance(table, dict) or set(table) != {"columns", "rows"}:
        raise ValueError("Missing or invalid designated final_table")
    columns, rows = table["columns"], table["rows"]
    if not isinstance(columns, list) or not columns or any(not isinstance(x, str) or not x.strip() for x in columns):
        raise ValueError("columns must be nonempty names")
    if len({x.strip().lower() for x in columns}) != len(columns):
        raise ValueError("Duplicate column names")
    if not isinstance(rows, list):
        raise ValueError("rows must be an array")
    for row in rows:
        if not isinstance(row, list) or len(row) != len(columns):
            raise ValueError("Row width mismatch")
        for cell in row:
            if isinstance(cell, (dict, list)) or (isinstance(cell, float) and not math.isfinite(cell)):
                raise ValueError("Cells must be finite JSON scalars or null")
    return json.dumps([dict(zip(columns, row)) for row in rows], ensure_ascii=False, allow_nan=False)
