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

import numpy as np
import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.moirai2.modeling_moirai2 import Moirai2ForPrediction

logger = Logger()


class Moirai2Pipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **infer_kwargs):
        """
        Preprocess input data for moirai2.

        Parameters
        ----------
        inputs : list of dict
            A list of dictionaries containing input data. Each dictionary contains:
            - 'targets': A tensor (1D or 2D) of shape (input_length,) or (target_count, input_length).

        infer_kwargs: Additional keyword arguments for inference, such as:
            - `output_length`(int): Prediction length.

        Returns
        -------
        list of dict
            Processed inputs compatible with moirai2 format (time, features).
        """
        super().preprocess(inputs, **infer_kwargs)
        # Moirai2.predict() expects past_target in (time, features) format
        processed_inputs = []
        for item in inputs:
            targets = item.get("targets", None)
            if targets is None:
                raise ValueError("Input must contain 'targets' key")

            if isinstance(targets, torch.Tensor):
                targets = targets.cpu().numpy()

            # Handle different input formats
            if targets.ndim == 1:
                # 1D: (input_length,) -> (input_length, 1)
                targets = targets[:, np.newaxis]
            elif targets.ndim == 2:
                # 2D: (target_count, input_length) -> (input_length, target_count)
                targets = targets.T

            processed_inputs.append({"past_target": targets})
        return processed_inputs

    def forecast(self, inputs, **infer_kwargs) -> list[torch.Tensor]:
        """
        Generate forecasts for the input time series.

        Parameters
        ----------
        inputs : list of dict
            Processed inputs from preprocess method.

        **infer_kwargs : Additional arguments for inference:
            - output_length (int): The length of the forecast output (default: 96).

        Returns
        -------
        list of torch.Tensor
            The model's predictions, each of shape (target_count, num_quantiles, prediction_length).

        Note
        ----
        All model parameters (prediction_length, context_length, target_dim) are
        dynamically determined from the actual input data and inference requirements.
        No pre-specification is needed during model loading.
        """
        prediction_length = infer_kwargs.get("output_length", 96)

        # Extract past_target from inputs
        past_target_list = [item["past_target"] for item in inputs]

        if isinstance(self.model, Moirai2ForPrediction):
            # After preprocess(), data is in (time, features) format
            first_sample = past_target_list[0]
            actual_context_length = first_sample.shape[0]  # time dimension
            target_dim = (
                first_sample.shape[1] if first_sample.ndim > 1 else 1
            )  # feature dimension

            # Use hparams_context to dynamically set ALL parameters
            with self.model.hparams_context(
                prediction_length=prediction_length,
                context_length=actual_context_length,
                target_dim=target_dim,
            ):
                predictions = self.model.predict(
                    past_target=past_target_list,
                    feat_dynamic_real=None,
                    past_feat_dynamic_real=None,
                )

            # Convert numpy array to torch tensor
            # predictions shape: (batch, num_quantiles, future_time, *tgt)
            # Note: past_target is ndarray (not tensor) because:
            # 1. Moirai2.predict() follows GluonTS interface standard (uses numpy)
            # 2. Internally, predict() automatically converts to tensor and moves to GPU
            # 3. This design maintains compatibility with GluonTS ecosystem
            predictions_list = []
            for i in range(predictions.shape[0]):
                pred = predictions[i]  # (num_quantiles, future_time, *tgt)
                # Transpose to (target_count, num_quantiles, prediction_length)
                if pred.ndim == 3:
                    pred = pred.transpose(
                        2, 0, 1
                    )  # (target_count, num_quantiles, future_time)
                elif pred.ndim == 2:
                    pred = pred[np.newaxis, :, :]  # (1, num_quantiles, future_time)
                predictions_list.append(torch.from_numpy(pred))
            return predictions_list
        else:
            raise ValueError(
                f"Model must be an instance of Moirai2ForPrediction, got {type(self.model)}"
            )

    def postprocess(
        self, outputs: list[torch.Tensor], **infer_kwargs
    ) -> list[torch.Tensor]:
        """
        Postprocesses the model's forecast outputs by selecting the 0.5 quantile or averaging over quantiles.

        Args:
            outputs (list[torch.Tensor]): List of forecast outputs, where each output is a 3D-tensor
                with shape [target_count, quantile_count, output_length].

        Returns:
            list[torch.Tensor]: Processed list of forecast outputs, each is a 2D-tensor
                with shape [target_count, output_length].
        """
        outputs_list = []
        for output in outputs:
            # Check if 0.5 quantile is available
            if output.shape[1] > 0:
                # Get the median quantile (middle quantile)
                median_idx = output.shape[1] // 2
                outputs_list.append(output[:, median_idx, :])
            else:
                # If no quantiles, get the mean
                outputs_list.append(output.mean(dim=1))
        super().postprocess(outputs_list, **infer_kwargs)
        return outputs_list
