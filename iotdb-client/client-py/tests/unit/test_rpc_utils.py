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

import numpy as np
import pandas as pd
import pytest

from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.rpc_utils import convert_to_timestamp


@pytest.mark.parametrize("precision", ["ms", "us", "ns"])
@pytest.mark.parametrize("value", [-(2**63) + 1, 2**63 - 1])
@pytest.mark.parametrize(
    "timezone", ["UTC", "Etc/UTC", "Asia/Shanghai", "America/New_York"]
)
@pytest.mark.parametrize("integer_type", [int, np.int64])
def test_convert_timestamp_preserves_extreme_value(
    precision, value, timezone, integer_type
):
    timestamp = convert_to_timestamp(integer_type(value), precision, timezone)
    assert isinstance(timestamp, pd.Timestamp)
    assert not pd.isna(timestamp)
    assert timestamp.asm8.astype(f"datetime64[{precision}]").astype(np.int64) == value


@pytest.mark.parametrize("precision", ["ms", "us", "ns"])
def test_convert_timestamp_preserves_nat(precision):
    assert convert_to_timestamp(-(2**63), precision, "Etc/UTC") is pd.NaT


@pytest.mark.parametrize(
    "timezone", ["UTC", "Etc/UTC", "Asia/Shanghai", "America/New_York"]
)
def test_convert_timestamp_preserves_normal_timezone(timezone):
    timestamp = convert_to_timestamp(1, "ms", timezone)
    expected = pd.Timestamp("1970-01-01T00:00:00.001Z").tz_convert(timezone)
    assert timestamp == expected
    assert timestamp.utcoffset() == expected.utcoffset()


def test_timestamp_field_formats_extreme_milliseconds():
    field = Field(TSDataType.TIMESTAMP, 2**63 - 1, timezone="Etc/UTC", precision="ms")
    assert (
        field.get_timestamp_value().asm8.astype("datetime64[ms]").astype(np.int64)
        == 2**63 - 1
    )
    suffix = "+00:00" if field.get_timestamp_value().tzinfo is not None else ""
    assert field.get_string_value() == "292278994-08-17T07:12:55.807" + suffix
