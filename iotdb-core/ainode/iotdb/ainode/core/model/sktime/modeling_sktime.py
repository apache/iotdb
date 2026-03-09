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

from abc import abstractmethod
from typing import Any, Dict

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sktime.detection.hmm_learn import GMMHMM, GaussianHMM
from sktime.detection.stray import STRAY
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.exp_smoothing import ExponentialSmoothing
from sktime.forecasting.naive import NaiveForecaster
from sktime.forecasting.trend import STLForecaster

from iotdb.ainode.core.exception import (
    BuiltInModelNotSupportException,
    InferenceModelInternalException,
)
from iotdb.ainode.core.log import Logger

from .configuration_sktime import get_attributes, update_attribute

logger = Logger()


class SktimeModel:
    """Base class for Sktime models"""

    def __init__(self, attributes: Dict[str, Any]):
        self._attributes = attributes
        self._model = None

    @abstractmethod
    def generate(self, data, **kwargs):
        """Execute generation/inference"""
        raise NotImplementedError


class ForecastingModel(SktimeModel):
    """Base class for forecasting models"""

    def generate(self, data, **kwargs):
        """Execute forecasting"""
        try:
            output_length = kwargs.get(
                "output_length", self._attributes["output_length"]
            )
            self._model.fit(data)
            output = self._model.predict(fh=range(output_length))
            return np.array(output, dtype=np.float64)
        except Exception as e:
            raise InferenceModelInternalException(str(e))


class DetectionModel(SktimeModel):
    """Base class for detection models"""

    def generate(self, data, **kwargs):
        """Execute detection"""
        try:
            output_length = kwargs.get("output_length", data.size)
            output = self._model.fit_transform(data[:output_length])
            if isinstance(output, pd.DataFrame):
                return np.array(output["labels"], dtype=np.int32)
            else:
                return np.array(output, dtype=np.int32)
        except Exception as e:
            raise InferenceModelInternalException(str(e))


class ArimaModel(ForecastingModel):
    """ARIMA model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = ARIMA(
            **{k: v for k, v in attributes.items() if k != "output_length"}
        )


class ExponentialSmoothingModel(ForecastingModel):
    """Exponential smoothing model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = ExponentialSmoothing(
            **{k: v for k, v in attributes.items() if k != "output_length"}
        )


class NaiveForecasterModel(ForecastingModel):
    """Naive forecaster model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = NaiveForecaster(
            **{k: v for k, v in attributes.items() if k != "output_length"}
        )


class STLForecasterModel(ForecastingModel):
    """STL forecaster model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = STLForecaster(
            **{k: v for k, v in attributes.items() if k != "output_length"}
        )


class GMMHMMModel(DetectionModel):
    """GMM HMM model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = GMMHMM(**attributes)


class GaussianHmmModel(DetectionModel):
    """Gaussian HMM model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = GaussianHMM(**attributes)


class STRAYModel(DetectionModel):
    """STRAY anomaly detection model"""

    def __init__(self, attributes: Dict[str, Any]):
        super().__init__(attributes)
        self._model = STRAY(**{k: v for k, v in attributes.items() if v is not None})

    def generate(self, data, **kwargs):
        """STRAY requires special handling: normalize first"""
        try:
            scaled_data = MinMaxScaler().fit_transform(data.values.reshape(-1, 1))
            scaled_data = pd.Series(scaled_data.flatten())
            return super().generate(scaled_data, **kwargs)
        except Exception as e:
            raise InferenceModelInternalException(str(e))


# Model factory mapping
_MODEL_FACTORY = {
    "ARIMA": ArimaModel,
    "EXPONENTIAL_SMOOTHING": ExponentialSmoothingModel,
    "HOLTWINTERS": ExponentialSmoothingModel,  # Use the same model class
    "NAIVE_FORECASTER": NaiveForecasterModel,
    "STL_FORECASTER": STLForecasterModel,
    "GMM_HMM": GMMHMMModel,
    "GAUSSIAN_HMM": GaussianHmmModel,
    "STRAY": STRAYModel,
}


def create_sktime_model(model_id: str, **kwargs) -> SktimeModel:
    """Create a Sktime model instance"""
    attributes = update_attribute({**kwargs}, get_attributes(model_id.upper()))
    model_class = _MODEL_FACTORY.get(model_id.upper())
    if model_class is None:
        raise BuiltInModelNotSupportException(model_id)
    return model_class(attributes)
