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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

import torch

from iotdb.ainode.core.inference.pool_group import PoolGroup
from iotdb.ainode.core.model.model_info import ModelInfo


class ScaleActionType(Enum):
    SCALE_UP = "Scale Up"
    SCALE_DOWN = "Scale Down"


@dataclass(frozen=True)
class ScaleAction:
    action: ScaleActionType
    amount: int
    model_id: str
    device_id: torch.device


class AbstractPoolScheduler(ABC):
    """
    Abstract base class for pool scheduling strategies.
    """

    def __init__(self, request_pool_map: Dict[str, Dict[torch.device, PoolGroup]]):
        """
        Args:
            request_pool_map: Dict["model_id", Dict[device_id, PoolGroup]].
        """
        self._request_pool_map = request_pool_map

    @abstractmethod
    def schedule(self, model_id: str) -> List[ScaleAction]:
        """
        Schedule a scaling action for the given model_id.
        """
        pass

    @abstractmethod
    def schedule_load_model_to_device(
        self, model_info: ModelInfo, device_id: torch.device
    ) -> List[ScaleAction]:
        """
        Schedule a series of actions to load the model to the device.
        Args:
            model_info: The model to be loaded.
            device_id: The device to load the model to.
        Returns:
            A list of ScaleAction to be performed.
        """
        pass

    @abstractmethod
    def schedule_unload_model_from_device(
        self, model_info: ModelInfo, device_id: torch.device
    ) -> List[ScaleAction]:
        """
        Schedule a series of actions to unload the model from the device.
        Args:
            model_info: The model to be unloaded.
            device_id: The device to unload the model from.
        Returns:
            A list of ScaleAction to be performed.
        """
        pass
