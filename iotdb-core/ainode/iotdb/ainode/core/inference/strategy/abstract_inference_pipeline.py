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
1
1import torch
1
1
1class AbstractInferencePipeline(ABC):
1    """
1    Abstract assistance strategy class for model inference.
1    This class shall define the interface process for specific model.
1    """
1
1    def __init__(self, model_config, **infer_kwargs):
1        self.model_config = model_config
1        self.infer_kwargs = infer_kwargs
1
1    @abstractmethod
1    def preprocess_inputs(self, inputs: torch.Tensor):
1        """
1        Preprocess the inputs before inference, including shape validation and value transformation.
1
1        Args:
1            inputs (torch.Tensor): The input tensor to be preprocessed.
1
1        Returns:
1            torch.Tensor: The preprocessed input tensor.
1        """
1        # TODO: Integrate with the data processing pipeline operators
1        pass
1
1    @abstractmethod
1    def post_decode(self):
1        """
1        Post-process the outputs after each decode step.
1        """
1        pass
1
1    @abstractmethod
1    def post_inference(self):
1        """
1        Post-process the outputs after the entire inference task.
1        """
1        pass
1