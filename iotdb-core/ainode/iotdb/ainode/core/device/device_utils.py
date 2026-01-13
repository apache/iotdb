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
from dataclasses import dataclass
from typing import Optional, Union

import torch

DeviceLike = Union[torch.device, str, int]


@dataclass(frozen=True)
class DeviceSpec:
    type: str
    index: Optional[int]


def parse_device_like(x: DeviceLike) -> DeviceSpec:
    if isinstance(x, int):
        return DeviceSpec("index", x)

    if isinstance(x, str):
        try:
            return DeviceSpec("index", int(x))
        except ValueError:
            s = x.strip().lower()
            if ":" in s:
                t, idx = s.split(":", 1)
                return DeviceSpec(t, int(idx))
            return DeviceSpec(s, None)

    if isinstance(x, torch.device):
        return DeviceSpec(x.type, x.index)

    raise TypeError(f"Unsupported device: {x!r}")
