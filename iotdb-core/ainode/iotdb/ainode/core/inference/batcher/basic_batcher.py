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
1from typing import List
1
1import torch
1
1from iotdb.ainode.core.inference.batcher.abstract_batcher import AbstractBatcher
1from iotdb.ainode.core.inference.inference_request import InferenceRequest
1
1
1class BasicBatcher(AbstractBatcher):
1    """
1    Basic batcher for inference requests.
1    """
1
1    def __init__(self):
1        """
1        Args:
1
1        """
1
1    def batch_request(self, reqs: List[InferenceRequest]) -> torch.Tensor:
1        """
1        Batch given requests by simply concatenating their inputs, only requests with uniformed output length can be batched.
1
1        - Considering the current implementation of AINode, we might merely be piecing together the input for now.
1
1        Args:
1            reqs (List[InferenceRequest]): List of inference requests.
1
1        Returns:
1            torch.Tensor: Concatenated input tensor of shape
1                          [sum(req.batch_size), length].
1        """
1        if not reqs:
1            raise ValueError("No requests provided to batch_request.")
1
1        # Ensure length consistency
1        length_set = {req.inputs.shape[1] for req in reqs}
1        if len(length_set) != 1:
1            raise ValueError(
1                f"All requests must have the same length, " f"but got {length_set}"
1            )
1
1        batch_inputs = torch.cat([req.inputs for req in reqs], dim=0)
1
1        return batch_inputs
1