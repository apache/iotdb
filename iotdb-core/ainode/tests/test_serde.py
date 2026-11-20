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
import struct
import time
import unittest

import pandas as pd
import torch

from iotdb.ainode.core.util import serde


def _reference_tensor_to_tsblock(data_tensor):
    """The previous implementation, value by value, kept as the reference for the byte layout."""
    if data_tensor.dim() == 0:
        data_tensor = data_tensor.unsqueeze(0).unsqueeze(0)
    elif data_tensor.dim() == 1:
        data_tensor = data_tensor.unsqueeze(0)
    data_frame = pd.DataFrame(data_tensor.cpu()).T
    position_count, value_column_size = data_frame.shape[0], data_frame.shape[1]
    keys = data_frame.keys()
    binary = value_column_size.to_bytes(4, byteorder="big")
    for data_type in data_frame.dtypes:
        binary += serde._get_type_in_byte(data_type)
    binary += position_count.to_bytes(4, byteorder="big")
    binary += b"\x02"
    for data_type in data_frame.dtypes:
        binary += serde._get_encoder(data_type)
    binary += bool.to_bytes(False, 1, byteorder="big")
    for _ in range(position_count):
        v = struct.pack(">i", 0)
        binary += v
        binary += v
    for i in range(value_column_size):
        binary += bool.to_bytes(False, 1, byteorder="big")
        col = data_frame[keys[i]]
        for j in range(position_count):
            value = col[j]
            if value.dtype.byteorder != ">":
                value = value.byteswap()
            binary += value.tobytes()
    return binary


class TestConvertTensorToTsBlock(unittest.TestCase):
    def test_same_bytes_as_reference(self):
        torch.manual_seed(0)
        cases = [
            torch.tensor(1.5),
            torch.randn(1),
            torch.randn(96),
            torch.randn(3, 96),
            torch.randn(4, 500),
            torch.randn(1000, dtype=torch.float64),
            torch.randn(2, 20000, dtype=torch.float64),
            torch.tensor([float("nan"), float("inf"), -0.0, 1.0]),
            torch.randint(-1000, 1000, (2, 50), dtype=torch.int32),
            torch.randint(-1000, 1000, (50,), dtype=torch.int64),
            torch.tensor([True, False, True]),
            torch.randn(3, 10).t(),  # not contiguous
        ]
        for tensor in cases:
            with self.subTest(shape=tuple(tensor.shape), dtype=tensor.dtype):
                self.assertEqual(
                    serde.convert_tensor_to_tsblock(tensor),
                    _reference_tensor_to_tsblock(tensor),
                )

    def test_long_series_is_fast(self):
        tensor = torch.randn(1_000_000, dtype=torch.float64)
        start = time.perf_counter()
        binary = serde.convert_tensor_to_tsblock(tensor)
        elapsed = time.perf_counter() - start
        # header (4 + 1 + 4 + 1 + 1), time column (1 + 8 per row), value column (1 + 8 per row)
        self.assertEqual(len(binary), 13 + 16 * 1_000_000)
        self.assertLess(elapsed, 5.0)


if __name__ == "__main__":
    unittest.main()
