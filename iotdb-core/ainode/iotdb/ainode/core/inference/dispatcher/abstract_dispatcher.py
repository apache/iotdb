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
from typing import Dict

from iotdb.ainode.core.inference.inference_request import InferenceRequest
from iotdb.ainode.core.inference.inference_request_pool import PoolState


class AbstractDispatcher(ABC):
    """
    Abstract base class for dispatchers that handle inference requests.
    """

    def __init__(self, pool_states: Dict[int, PoolState]):
        """
        Args:
            pool_states: Dictionary containing the states of inference request pools in the same pool group.
        """
        self.pool_states = pool_states

    @abstractmethod
    def dispatch_request(self, req: InferenceRequest, pool_ids: list[int]) -> int:
        """
        Dispatch an inference request to the appropriate pool.
        """
        pass
