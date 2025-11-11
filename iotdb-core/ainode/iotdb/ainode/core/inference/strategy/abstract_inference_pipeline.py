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

import torch


class AbstractInferencePipeline(ABC):
    """
    Abstract assistance strategy class for model inference.
    This class shall define the interface process for specific model.
    """

    def __init__(self, model_config, **infer_kwargs):
        self.model_config = model_config
        self.infer_kwargs = infer_kwargs

    @abstractmethod
    def preprocess_inputs(self, inputs: torch.Tensor):
        """
        Preprocess the inputs before inference, including shape validation and value transformation.

        Args:
            inputs (torch.Tensor): The input tensor to be preprocessed.

        Returns:
            torch.Tensor: The preprocessed input tensor.
        """
        # TODO: Integrate with the data processing pipeline operators
        pass

    @abstractmethod
    def post_decode(self):
        """
        Post-process the outputs after each decode step.
        """
        pass

    @abstractmethod
    def post_inference(self):
        """
        Post-process the outputs after the entire inference task.
        """
        pass
