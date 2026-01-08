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

from enum import Enum
from typing import ContextManager, Optional, Protocol

import torch


class BackendType(Enum):
    """
    Different types of supported computation backends.
    AINode will automatically select the available backend according to the order defined here.
    """

    CUDA = "cuda"
    CPU = "cpu"


class BackendAdapter(Protocol):
    type: BackendType

    # device basics
    def is_available(self) -> bool: ...
    def device_count(self) -> int: ...
    def make_device(self, index: Optional[int]) -> torch.device: ...
    def set_device(self, index: int) -> None: ...
