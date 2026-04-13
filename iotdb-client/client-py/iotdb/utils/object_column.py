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
"""Encode/decode OBJECT column cell payloads (aligned with Java/C++ Tablet.addValue)."""

import struct
from typing import Tuple


def encode_object_cell(is_eof: bool, offset: int, content: bytes) -> bytes:
    """
    OBJECT cell on the wire (before length-prefix in tablet column buffer):
    [is_eof: 1 byte 0/1][offset: 8 bytes big-endian int64][payload...]
    """
    if not isinstance(content, (bytes, bytearray)):
        raise TypeError("content must be bytes")
    flag = b"\x01" if is_eof else b"\x00"
    return flag + struct.pack(">q", offset) + bytes(content)


def decode_object_cell(cell: bytes) -> Tuple[bool, int, bytes]:
    """Inverse of encode_object_cell."""
    if len(cell) < 9:
        raise ValueError("OBJECT cell too short")
    is_eof = cell[0] != 0
    offset = struct.unpack(">q", cell[1:9])[0]
    return is_eof, offset, cell[9:]
