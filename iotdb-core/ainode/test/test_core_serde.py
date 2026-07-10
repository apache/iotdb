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

import sys
import types

import numpy as np
import torch

tsfile_stub = types.ModuleType("iotdb.tsfile")
tsfile_utils_stub = types.ModuleType("iotdb.tsfile.utils")
tsblock_serde_stub = types.ModuleType("iotdb.tsfile.utils.tsblock_serde")
tsblock_serde_stub.deserialize = lambda _: None
sys.modules["iotdb.tsfile"] = tsfile_stub
sys.modules["iotdb.tsfile.utils"] = tsfile_utils_stub
sys.modules["iotdb.tsfile.utils.tsblock_serde"] = tsblock_serde_stub

from iotdb.ainode.core.util import serde as core_serde
from timecho.ainode.core.util import serde as timecho_serde


def test_convert_tsblock_to_tensor_expands_null_values(monkeypatch):
    monkeypatch.setattr(
        core_serde,
        "deserialize",
        lambda _: (
            np.array([1, 2, 3], dtype=">i8"),
            [np.array([10.0, 30.0], dtype=">f8")],
            [np.array([False, True, False])],
            3,
        ),
    )

    tensor = core_serde.convert_tsblock_to_tensor(b"unused")

    assert tensor.shape == (1, 1, 3)
    assert tensor[0, 0, 0].item() == 10.0
    assert torch.isnan(tensor[0, 0, 1])
    assert tensor[0, 0, 2].item() == 30.0


def test_timecho_convert_tsblock_to_tensor_and_timestamps_expands_null_values(
    monkeypatch,
):
    monkeypatch.setattr(
        timecho_serde,
        "deserialize",
        lambda _: (
            np.array([1, 2, 3], dtype=">i8"),
            [
                np.array([10.0, 30.0], dtype=">f8"),
                np.array([20.0, 40.0], dtype=">f8"),
            ],
            [
                np.array([False, True, False]),
                np.array([True, False, False]),
            ],
            3,
        ),
    )

    tensor, timestamps = timecho_serde.convert_tsblock_to_tensor_and_timestamps(
        b"unused"
    )

    assert timestamps == [1, 2, 3]
    assert tensor.shape == (1, 2, 3)
    assert torch.equal(
        torch.nan_to_num(tensor[0, 0], nan=-1.0), torch.tensor([10.0, -1.0, 30.0])
    )
    assert torch.equal(
        torch.nan_to_num(tensor[0, 1], nan=-1.0), torch.tensor([-1.0, 20.0, 40.0])
    )
