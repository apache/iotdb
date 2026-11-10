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

from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from iotdb.utils.rpc_utils import convert_to_timestamp, isoformat


@pytest.mark.parametrize("precision", ["ms", "us", "ns"])
@pytest.mark.parametrize("value", [-(2**63) + 1, 2**63 - 1])
def test_timestamp_boundaries_keep_value_and_precision(precision, value):
    for scalar in (value, np.int64(value)):
        for timezone in ("UTC", "Etc/UTC"):
            timestamp = convert_to_timestamp(scalar, precision, timezone)
            assert timestamp.asm8.dtype == np.dtype(f"datetime64[{precision}]")
            assert timestamp.asm8.astype(np.int64) == value
            if timestamp.tzinfo is not None:
                assert timestamp.utcoffset() == timedelta(0)
            expected = np.datetime_as_string(np.datetime64(value, precision))
            formatted = isoformat(timestamp, precision)
            if formatted.endswith("+00:00"):
                formatted = formatted[:-6]
            assert formatted == expected


@pytest.mark.parametrize("precision", ["ms", "us", "ns"])
@pytest.mark.parametrize("timezone", ["UTC", "Asia/Shanghai"])
def test_normal_timestamp_keeps_timezone(precision, timezone):
    value = 123456789
    timestamp = convert_to_timestamp(value, precision, timezone)
    assert timestamp == pd.Timestamp(value, unit=precision, tz=timezone)
    assert str(timestamp.tzinfo) == timezone


def test_timestamp_nat_is_preserved():
    for precision in ("ms", "us", "ns"):
        assert convert_to_timestamp(-(2**63), precision, "Etc/UTC") is pd.NaT
