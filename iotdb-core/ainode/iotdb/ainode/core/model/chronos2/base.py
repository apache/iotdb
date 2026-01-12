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

from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import torch

if TYPE_CHECKING:
    import pandas as pd
    from transformers import PreTrainedModel


from iotdb.ainode.core.model.chronos2.utils import left_pad_and_stack_1D


class ForecastType(Enum):
    SAMPLES = "samples"
    QUANTILES = "quantiles"


class PipelineRegistry(type):
    REGISTRY: Dict[str, "PipelineRegistry"] = {}

    def __new__(cls, name, bases, attrs):
        """See, https://github.com/faif/python-patterns."""
        new_cls = type.__new__(cls, name, bases, attrs)
        if name is not None:
            cls.REGISTRY[name] = new_cls

        return new_cls


class BaseChronosPipeline(metaclass=PipelineRegistry):
    forecast_type: ForecastType
    dtypes = {"bfloat16": torch.bfloat16, "float32": torch.float32}

    def __init__(self, inner_model: "PreTrainedModel"):
        """
        Parameters
        ----------
        inner_model : PreTrainedModel
            A hugging-face transformers PreTrainedModel, e.g., T5ForConditionalGeneration
        """
        # for easy access to the inner HF-style model
        self.inner_model = inner_model

    @property
    def model_context_length(self) -> int:
        raise NotImplementedError()

    @property
    def model_prediction_length(self) -> int:
        raise NotImplementedError()

    def _prepare_and_validate_context(
        self, context: Union[torch.Tensor, List[torch.Tensor]]
    ):
        if isinstance(context, list):
            context = left_pad_and_stack_1D(context)
        assert isinstance(context, torch.Tensor)
        if context.ndim == 1:
            context = context.unsqueeze(0)
        assert context.ndim == 2

        return context

    def predict(
        self,
        inputs: Union[torch.Tensor, List[torch.Tensor]],
        prediction_length: Optional[int] = None,
    ):
        """
        Get forecasts for the given time series. Predictions will be
        returned in fp32 on the cpu.

        Parameters
        ----------
        inputs
            Input series. This is either a 1D tensor, or a list
            of 1D tensors, or a 2D tensor whose first dimension
            is batch. In the latter case, use left-padding with
            ``torch.nan`` to align series of different lengths.
        prediction_length
            Time steps to predict. Defaults to a model-dependent
            value if not given.

        Returns
        -------
        forecasts
            Tensor containing forecasts. The layout and meaning
            of the forecasts values depends on ``self.forecast_type``.
        """
        raise NotImplementedError()

    def predict_quantiles(
        self,
        inputs: Union[torch.Tensor, List[torch.Tensor]],
        prediction_length: Optional[int] = None,
        quantile_levels: List[float] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        **kwargs,
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Get quantile and mean forecasts for given time series.
        Predictions will be returned in fp32 on the cpu.

        Parameters
        ----------
        inputs : Union[torch.Tensor, List[torch.Tensor]]
            Input series. This is either a 1D tensor, or a list
            of 1D tensors, or a 2D tensor whose first dimension
            is batch. In the latter case, use left-padding with
            ``torch.nan`` to align series of different lengths.
        prediction_length : Optional[int], optional
            Time steps to predict. Defaults to a model-dependent
            value if not given.
        quantile_levels : List[float], optional
            Quantile levels to compute, by default [0.1, 0.2, ..., 0.9]

        Returns
        -------
        quantiles
            Tensor containing quantile forecasts. Shape
            (batch_size, prediction_length, num_quantiles)
        mean
            Tensor containing mean (point) forecasts. Shape
            (batch_size, prediction_length)
        """
        raise NotImplementedError()

    def predict_df(
        self,
        df: "pd.DataFrame",
        *,
        id_column: str = "item_id",
        timestamp_column: str = "timestamp",
        target: str = "target",
        prediction_length: int | None = None,
        quantile_levels: list[float] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        validate_inputs: bool = True,
        **predict_kwargs,
    ) -> "pd.DataFrame":
        """
        Perform forecasting on time series data in a long-format pandas DataFrame.

        Parameters
        ----------
        df
            Time series data in long format with an id column, a timestamp, and one target column.
            Any other columns, if present, will be ignored
        id_column
            The name of the column which contains the unique time series identifiers, by default "item_id"
        timestamp_column
            The name of the column which contains timestamps, by default "timestamp"
            All time series in the dataframe must have regular timestamps with the same frequency (no gaps)
        target
            The name of the column which contains the target variables to be forecasted, by default "target"
        prediction_length
            Number of steps to predict for each time series
        quantile_levels
            Quantile levels to compute
        validate_inputs
            When True, the dataframe(s) will be validated before prediction, ensuring that timestamps have a
            regular frequency, and item IDs match between past and future data. Setting to False disables these checks.
        **predict_kwargs
            Additional arguments passed to predict_quantiles

        Returns
        -------
        The forecasts dataframe generated by the model with the following columns
        - `id_column`: The time series ID
        - `timestamp_column`: Future timestamps
        - "target_name": The name of the target column
        - "predictions": The point predictions generated by the model
        - One column for predictions at each quantile level in `quantile_levels`
        """
        try:
            import pandas as pd

            from .df_utils import convert_df_input_to_list_of_dicts_input
        except ImportError:
            raise ImportError(
                "pandas is required for predict_df. Please install it with `pip install pandas`."
            )

        if not isinstance(target, str):
            raise ValueError(
                f"Expected `target` to be str, but found {type(target)}. {self.__class__.__name__} only supports univariate forecasting."
            )

        if prediction_length is None:
            prediction_length = self.model_prediction_length

        inputs, original_order, prediction_timestamps = (
            convert_df_input_to_list_of_dicts_input(
                df=df,
                future_df=None,
                id_column=id_column,
                timestamp_column=timestamp_column,
                target_columns=[target],
                prediction_length=prediction_length,
                validate_inputs=validate_inputs,
            )
        )

        # NOTE: any covariates, if present, are ignored here
        context = [
            torch.tensor(item["target"]).squeeze(0) for item in inputs
        ]  # squeeze the extra variate dim

        # Generate forecasts
        quantiles, mean = self.predict_quantiles(
            inputs=context,
            prediction_length=prediction_length,
            quantile_levels=quantile_levels,
            limit_prediction_length=False,
            **predict_kwargs,
        )

        quantiles_np = quantiles.numpy()  # [n_series, horizon, num_quantiles]
        mean_np = mean.numpy()  # [n_series, horizon]

        results_dfs = []
        for i, (series_id, future_ts) in enumerate(prediction_timestamps.items()):
            q_pred = quantiles_np[i]  # (horizon, num_quantiles)
            point_pred = mean_np[i]  # (horizon)

            series_forecast_data = {
                id_column: series_id,
                timestamp_column: future_ts,
                "target_name": target,
            }
            series_forecast_data["predictions"] = point_pred
            for q_idx, q_level in enumerate(quantile_levels):
                series_forecast_data[str(q_level)] = q_pred[:, q_idx]

            results_dfs.append(pd.DataFrame(series_forecast_data))

        predictions_df = pd.concat(results_dfs, ignore_index=True)
        predictions_df.set_index(id_column, inplace=True)
        predictions_df = predictions_df.loc[original_order]
        predictions_df.reset_index(inplace=True)

        return predictions_df

    @classmethod
    def from_pretrained(
        cls,
        pretrained_model_name_or_path: Union[str, Path],
        *model_args,
        force_s3_download=False,
        **kwargs,
    ):
        """
        Load the model, either from a local path, S3 prefix, or from the HuggingFace Hub.
        Supports the same arguments as ``AutoConfig`` and ``AutoModel`` from ``transformers``.
        """

        from transformers import AutoConfig

        torch_dtype = kwargs.get("torch_dtype", "auto")
        if torch_dtype != "auto" and isinstance(torch_dtype, str):
            kwargs["torch_dtype"] = cls.dtypes[torch_dtype]

        config = AutoConfig.from_pretrained(pretrained_model_name_or_path, **kwargs)
        is_valid_config = hasattr(config, "chronos_pipeline_class") or hasattr(
            config, "chronos_config"
        )

        if not is_valid_config:
            raise ValueError("Not a Chronos config file")

        pipeline_class_name = getattr(
            config, "chronos_pipeline_class", "ChronosPipeline"
        )
        class_ = PipelineRegistry.REGISTRY.get(pipeline_class_name)
        if class_ is None:
            raise ValueError(
                f"Trying to load unknown pipeline class: {pipeline_class_name}"
            )

        return class_.from_pretrained(  # type: ignore[attr-defined]
            pretrained_model_name_or_path, *model_args, **kwargs
        )
