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
1from typing import Dict
1
1from iotdb.ainode.core.inference.inference_request import InferenceRequest
1from iotdb.ainode.core.inference.inference_request_pool import PoolState
1
1
1class AbstractDispatcher(ABC):
1    """
1    Abstract base class for dispatchers that handle inference requests.
1    """
1
1    def __init__(self, pool_states: Dict[int, PoolState]):
1        """
1        Args:
1            pool_states: Dictionary containing the states of inference request pools in the same pool group.
1        """
1        self.pool_states = pool_states
1
1    @abstractmethod
1    def dispatch_request(self, req: InferenceRequest, pool_ids: list[int]) -> int:
1        """
1        Dispatch an inference request to the appropriate pool.
1        """
1        pass
1