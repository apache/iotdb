1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1from abc import ABC, abstractmethod
1from dataclasses import dataclass
1from enum import Enum
1from typing import Dict, List
1
1from iotdb.ainode.core.inference.pool_group import PoolGroup
1
1
1class ScaleActionType(Enum):
1    SCALE_UP = "Scale Up"
1    SCALE_DOWN = "Scale Down"
1
1
1@dataclass(frozen=True)
1class ScaleAction:
1    action: ScaleActionType
1    amount: int
1    model_id: str
1    device_id: str
1
1
1class AbstractPoolScheduler(ABC):
1    """
1    Abstract base class for pool scheduling strategies.
1    """
1
1    def __init__(self, request_pool_map: Dict[str, Dict[str, PoolGroup]]):
1        """
1        Args:
1            request_pool_map: Dict["model_id", Dict["device_id", PoolGroup]].
1        """
1        self._request_pool_map = request_pool_map
1
1    @abstractmethod
1    def schedule(self, model_id: str) -> List[ScaleAction]:
1        """
1        Schedule a scaling action for the given model_id.
1        """
1        pass
1
1    @abstractmethod
1    def schedule_load_model_to_device(
1        self, model_id: str, device_id: str
1    ) -> List[ScaleAction]:
1        """
1        Schedule a series of actions to load the model to the device.
1        Args:
1            model_id: The model to be loaded.
1            device_id: The device to load the model to.
1        Returns:
1            A list of ScaleAction to be performed.
1        """
1        pass
1
1    @abstractmethod
1    def schedule_unload_model_from_device(
1        self, model_id: str, device_id: str
1    ) -> List[ScaleAction]:
1        """
1        Schedule a series of actions to unload the model from the device.
1        Args:
1            model_id: The model to be unloaded.
1            device_id: The device to unload the model from.
1        Returns:
1            A list of ScaleAction to be performed.
1        """
1        pass
1