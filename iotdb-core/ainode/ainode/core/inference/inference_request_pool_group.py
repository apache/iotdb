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
from typing import Dict, Tuple

import torch.multiprocessing as mp

from ainode.core.exception import (
    InferenceModelInternalError,
)
from ainode.core.inference.inference_request_pool import InferenceRequestPool
from ainode.core.log import Logger

logger = Logger()


class PoolGroup:
    """
    A group of inference request pools for a specific model.
    """

    def __init__(self, model_id):
        self.pool_group: Dict[int, Tuple[InferenceRequestPool, mp.Queue]] = {}
        self.model_id = model_id

    def get_pool_group(self) -> Dict[int, Tuple[InferenceRequestPool, mp.Queue]]:
        return self.pool_group

    def add_pool(
        self, pool_id: int, request_pool: InferenceRequestPool, request_queue: mp.Queue
    ):
        self.pool_group[pool_id] = (request_pool, request_queue)

    def get_pool_ids(self) -> list[int]:
        return list(self.pool_group.keys())

    def get_request_pool(self, pool_id) -> InferenceRequestPool:
        if pool_id not in self.pool_group:
            raise InferenceModelInternalError(
                f"Pool ID {pool_id} not found for model {self.model_id}"
            )
        return self.pool_group[pool_id][0]

    def get_request_queue(self, pool_id) -> mp.Queue:
        if pool_id not in self.pool_group:
            raise InferenceModelInternalError(
                f"Pool ID {pool_id} not found for model {self.model_id}"
            )
        return self.pool_group[pool_id][1]
