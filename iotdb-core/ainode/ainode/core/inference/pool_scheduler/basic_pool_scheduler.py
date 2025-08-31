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

from typing import Dict, List

import torch

from ainode.core.exception import (
    InferenceModelInternalError,
)
from ainode.core.inference.pool_group import PoolGroup
from ainode.core.inference.pool_scheduler.abstract_pool_scheduler import (
    AbstractPoolScheduler,
    ScaleAction,
    ScaleActionType,
)
from ainode.core.log import Logger
from ainode.core.manager.utils import estimate_pool_size

logger = Logger()


class BasicPoolScheduler(AbstractPoolScheduler):
    """
    A basic scheduler to init the request pools.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self, request_pool_map: Dict[str, PoolGroup]):
        super().__init__(request_pool_map)

    def schedule(self, model_id: str) -> List[ScaleAction]:
        """
        Schedule a scaling action for the given model_id.
        """
        if model_id not in self._request_pool_map:
            pool_num = estimate_pool_size(self.DEFAULT_DEVICE, model_id)
            if pool_num <= 0:
                raise InferenceModelInternalError(
                    f"Not enough memory to run model {model_id}."
                )
            return [ScaleAction(ScaleActionType.SCALE_UP, pool_num, model_id)]
