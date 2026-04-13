# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import pytest

from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet, ColumnType
from iotdb.utils.object_column import decode_object_cell, encode_object_cell


def test_encode_decode_object_cell_roundtrip():
    payload = b"hello-object"
    cell = encode_object_cell(True, 0, payload)
    is_eof, offset, body = decode_object_cell(cell)
    assert is_eof is True
    assert offset == 0
    assert body == payload


def test_encode_object_cell_segment():
    cell = encode_object_cell(False, 512, b"ab")
    is_eof, offset, body = decode_object_cell(cell)
    assert is_eof is False
    assert offset == 512
    assert body == b"ab"


def test_tablet_add_value_object_and_pack():
    column_names = ["region_id", "plant_id", "device_id", "temperature", "file"]
    data_types = [
        TSDataType.STRING,
        TSDataType.STRING,
        TSDataType.STRING,
        TSDataType.FLOAT,
        TSDataType.OBJECT,
    ]
    column_types = [
        ColumnType.TAG,
        ColumnType.TAG,
        ColumnType.TAG,
        ColumnType.FIELD,
        ColumnType.FIELD,
    ]
    timestamps = [1]
    values = [["1", "5", "3", 37.6, None]]
    tablet = Tablet(
        "object_table", column_names, data_types, values, timestamps, column_types
    )
    tablet.add_value_object(0, 4, True, 0, b"payload-bytes")
    assert tablet.get_binary_values() is not None


def test_tablet_add_value_object_wrong_column():
    column_names = ["a"]
    data_types = [TSDataType.STRING]
    tablet = Tablet("t", column_names, data_types, [["x"]], [1])
    with pytest.raises(TypeError):
        tablet.add_value_object(0, 0, True, 0, b"x")
