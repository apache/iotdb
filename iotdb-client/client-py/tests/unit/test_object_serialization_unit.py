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

import struct

from iotdb.Session import Session
from iotdb.utils.Field import Field, parse_object_byte_array_to_string
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet


def test_object_serialization_and_formatting_should_match_tsfile_semantics():
    # The server-side parse rule only cares about the first 8 bytes:
    # BytesUtils.parseObjectByteArrayToString -> objectSize (big-endian unsigned long)
    object_size = 67108864  # 64MB
    stored_object_bytes = (
        object_size.to_bytes(8, byteorder="big", signed=False) + b"rest"
    )
    expected_string = parse_object_byte_array_to_string(stored_object_bytes)

    # 1) Parsing/formatting (no server)
    field = Field(TSDataType.OBJECT, stored_object_bytes)
    assert field.get_binary_value() == stored_object_bytes
    assert field.get_string_value() == expected_string
    assert field.get_object_value(TSDataType.OBJECT) == expected_string

    # 2) Tablet serialization (insertTablet)
    # For OBJECT-like binary columns, Tablet packs: [int length][bytes] per row.
    tablet = Tablet(
        "root.sg_py_object_unit",
        ["obj_m1"],
        [TSDataType.OBJECT],
        [[stored_object_bytes]],
        [100],
    )
    actual_tablet_binary_values = tablet.get_binary_values()
    expected_tablet_binary_values = struct.pack(
        f">i{len(stored_object_bytes)}s",
        len(stored_object_bytes),
        stored_object_bytes,
    )
    assert actual_tablet_binary_values == expected_tablet_binary_values

    # 3) Record serialization (insertRecords style): Session.value_to_bytes
    # OBJECT branch uses an extra type marker: b"\x0c" followed by [int length][bytes].
    actual_record_bytes = Session.value_to_bytes(
        [TSDataType.OBJECT], [stored_object_bytes]
    )
    expected_record_bytes = struct.pack(
        f">ci{len(stored_object_bytes)}s",
        b"\x0c",
        len(stored_object_bytes),
        stored_object_bytes,
    )
    assert actual_record_bytes == expected_record_bytes
