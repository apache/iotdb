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
from typing import List


class BuiltInModelType(Enum):
    # forecast models
    ARIMA = "Arima"
    HOLTWINTERS = "HoltWinters"
    EXPONENTIAL_SMOOTHING = "ExponentialSmoothing"
    NAIVE_FORECASTER = "NaiveForecaster"
    STL_FORECASTER = "StlForecaster"

    # anomaly detection models
    GAUSSIAN_HMM = "GaussianHmm"
    GMM_HMM = "GmmHmm"
    STRAY = "Stray"

    # large time series models (LTSM)
    TIMER_XL = "Timer-XL"
    # sundial
    SUNDIAL = "Timer-Sundial"

    @classmethod
    def values(cls) -> List[str]:
        return [item.value for item in cls]

    @staticmethod
    def is_built_in_model(model_type: str) -> bool:
        """
        Check if the given model type corresponds to a built-in model.
        """
        return model_type in BuiltInModelType.values()


class ModelFileType(Enum):
    SAFETENSORS = "safetensors"
    PYTORCH = "pytorch"
    UNKNOWN = "unknown"


class ModelCategory(Enum):
    BUILT_IN = "BUILT-IN"
    FINE_TUNED = "FINE-TUNED"
    USER_DEFINED = "USER-DEFINED"


class ModelStates(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    LOADING = "LOADING"
    DROPPING = "DROPPING"
    TRAINING = "TRAINING"
    FAILED = "FAILED"
