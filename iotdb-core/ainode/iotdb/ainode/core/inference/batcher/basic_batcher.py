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

from typing import List

import torch

from iotdb.ainode.core.inference.batcher.abstract_batcher import AbstractBatcher
from iotdb.ainode.core.inference.inference_request import InferenceRequest


class BasicBatcher(AbstractBatcher):
    """
    Basic batcher for inference requests.
    """

    def __init__(self):
        """
        Args:

        """

    def batch_request(self, reqs: List[InferenceRequest]) -> torch.Tensor:
        """
        Batch given requests by simply concatenating their inputs, only requests with uniformed output length can be batched.

        - Considering the current implementation of AINode, we might merely be piecing together the input for now.

        Args:
            reqs (List[InferenceRequest]): List of inference requests.

        Returns:
            torch.Tensor: Concatenated input tensor of shape
                          [sum(req.batch_size), length].
        """
        if not reqs:
            raise ValueError("No requests provided to batch_request.")

        # Ensure length consistency
        length_set = {req.inputs.shape[1] for req in reqs}
        if len(length_set) != 1:
            raise ValueError(
                f"All requests must have the same length, " f"but got {length_set}"
            )

        batch_inputs = torch.cat([req.inputs for req in reqs], dim=0)

        return batch_inputs
