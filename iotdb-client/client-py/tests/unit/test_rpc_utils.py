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

from iotdb.utils.rpc_utils import convert_to_timestamp


@pytest.mark.parametrize("value", [-(2**63) + 1, 2**63 - 1])
@pytest.mark.parametrize("timezone", ["UTC", "Asia/Shanghai"])
def test_convert_timestamp_preserves_extreme_milliseconds(value, timezone):
    timestamp = convert_to_timestamp(value, "ms", timezone)
    assert isinstance(timestamp, pd.Timestamp)
    assert not pd.isna(timestamp)
    assert timestamp.asm8.astype("datetime64[ms]").astype(np.int64) == value
