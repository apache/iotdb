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

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.manager.device_manager import DeviceManager
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.model_loader import load_model

BACKEND = DeviceManager()


class BasicPipeline(ABC):
    def __init__(self, model_info: ModelInfo, **model_kwargs):
        self.model_info = model_info
        self.device = model_kwargs.get("device", BACKEND.torch_device("cpu"))
        self.model = load_model(model_info, device_map=self.device, **model_kwargs)

    @abstractmethod
    def preprocess(self, inputs, **infer_kwargs):
        """
        Preprocess the input before inference, including shape validation and value transformation.
        """
        raise NotImplementedError("preprocess not implemented")

    @abstractmethod
    def postprocess(self, outputs, **infer_kwargs):
        """
        Post-process the outputs after the entire inference task.
        """
        raise NotImplementedError("postprocess not implemented")


class ForecastPipeline(BasicPipeline):
    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(
        self,
        inputs: list[dict[str, dict[str, torch.Tensor] | torch.Tensor]],
        **infer_kwargs,
    ):
        """
        Preprocess the input data before passing it to the model for inference, validating the shape and type of the input data.

        Args:
            inputs (list[dict]):
                The input data, a list of dictionaries, where each dictionary contains:
                    - 'targets': A tensor (1D or 2D) of shape (input_length,) or (target_count, input_length).
                    - 'past_covariates': A dictionary of tensors (optional), where each tensor has shape (input_length,).
                    - 'future_covariates': A dictionary of tensors (optional), where each tensor has shape (input_length,).

            infer_kwargs (dict, optional): Additional keyword arguments for inference, such as:
                - `output_length`(int): Used to check validation of 'future_covariates' if provided.

        Raises:
            ValueError: If the input format is incorrect (e.g., missing keys, invalid tensor shapes).

        Returns:
            The preprocessed inputs, validated and ready for model inference.
        """

        if isinstance(inputs, list):
            output_length = infer_kwargs.get("output_length", 96)
            for idx, input_dict in enumerate(inputs):
                # Check if the dictionary contains the expected keys
                if not isinstance(input_dict, dict):
                    raise ValueError(f"Input at index {idx} is not a dictionary.")

                required_keys = ["targets"]
                for key in required_keys:
                    if key not in input_dict:
                        raise ValueError(
                            f"Key '{key}' is missing in input at index {idx}."
                        )

                # Check 'targets' is torch.Tensor and has the correct shape
                targets = input_dict["targets"]
                if not isinstance(targets, torch.Tensor):
                    raise ValueError(
                        f"'targets' must be torch.Tensor, but got {type(targets)} at index {idx}."
                    )
                if targets.ndim not in [1, 2]:
                    raise ValueError(
                        f"'targets' must have 1 or 2 dimensions, but got {targets.ndim} dimensions at index {idx}."
                    )
                # If targets is 2-d, check if the second dimension is input_length
                if targets.ndim == 2:
                    n_variates, input_length = targets.shape
                else:
                    input_length = targets.shape[
                        0
                    ]  # for 1-d targets, shape should be (input_length,)

                # Check 'past_covariates' if it exists (optional)
                past_covariates = input_dict.get("past_covariates", {})
                if not isinstance(past_covariates, dict):
                    raise ValueError(
                        f"'past_covariates' must be a dictionary, but got {type(past_covariates)} at index {idx}."
                    )
                for cov_key, cov_value in past_covariates.items():
                    if not isinstance(cov_value, torch.Tensor):
                        raise ValueError(
                            f"Each value in 'past_covariates' must be torch.Tensor, but got {type(cov_value)} for key '{cov_key}' at index {idx}."
                        )
                    if cov_value.ndim != 1 or cov_value.shape[0] != input_length:
                        raise ValueError(
                            f"Each covariate in 'past_covariates' must have shape ({input_length},), but got shape {cov_value.shape} for key '{cov_key}' at index {idx}."
                        )

                # Check 'future_covariates' if it exists (optional)
                future_covariates = input_dict.get("future_covariates", {})
                if not isinstance(future_covariates, dict):
                    raise ValueError(
                        f"'future_covariates' must be a dictionary, but got {type(future_covariates)} at index {idx}."
                    )
                # If future_covariates exists, check if they are a subset of past_covariates
                if future_covariates:
                    for cov_key, cov_value in future_covariates.items():
                        if cov_key not in past_covariates:
                            raise ValueError(
                                f"Key '{cov_key}' in 'future_covariates' is not in 'past_covariates' at index {idx}."
                            )
                        if not isinstance(cov_value, torch.Tensor):
                            raise ValueError(
                                f"Each value in 'future_covariates' must be torch.Tensor, but got {type(cov_value)} for key '{cov_key}' at index {idx}."
                            )
                        if cov_value.ndim != 1 or cov_value.shape[0] != output_length:
                            raise ValueError(
                                f"Each covariate in 'future_covariates' must have shape ({output_length},), but got shape {cov_value.shape} for key '{cov_key}' at index {idx}."
                            )
        else:
            raise ValueError(
                f"The inputs must be a list of dictionaries, but got {type(inputs)}."
            )
        return inputs

    @abstractmethod
    def forecast(self, inputs, **infer_kwargs):
        """
        Perform forecasting on the given inputs.

        Parameters:
            inputs: The input data used for making predictions. The type and structure
                    depend on the specific implementation of the model.
            **infer_kwargs: Additional inference parameters such as:
                - `output_length`(int): The number of time points that model should generate.

        Returns:
            The forecasted output, which will depend on the specific model's implementation.
        """
        pass

    def postprocess(
        self, outputs: list[torch.Tensor], **infer_kwargs
    ) -> list[torch.Tensor]:
        """
        Postprocess the model outputs after inference, validating the shape of the output data and ensures it matches the expected dimensions.

        Args:
            outputs:
                The model outputs, which is a list of 2D tensors, where each tensor has shape `[target_count, output_length]`.

        Raises:
            InferenceModelInternalException: If the output tensor has an invalid shape (e.g., wrong number of dimensions).
            ValueError: If the output format is incorrect.

        Returns:
            list[torch.Tensor]:
                The postprocessed outputs, which will be a list of 2D tensors.
        """
        if isinstance(outputs, list):
            for idx, output in enumerate(outputs):
                if output.ndim != 2:
                    raise InferenceModelInternalException(
                        f"Output in outputs_list should be 2D-tensor, but receives {output.ndim} dims at index {idx}."
                    )
        else:
            raise ValueError(
                f"The outputs should be a list of 2D-tensors, but got {type(outputs)}."
            )
        return outputs


class ClassificationPipeline(BasicPipeline):
    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **kwargs):
        return inputs

    @abstractmethod
    def classify(self, inputs, **kwargs):
        pass

    def postprocess(self, outputs, **kwargs):
        return outputs


class ChatPipeline(BasicPipeline):
    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **kwargs):
        return inputs

    @abstractmethod
    def chat(self, inputs, **kwargs):
        pass

    def postprocess(self, outputs, **kwargs):
        return outputs
