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


def get_built_in_model_type(model_type: str) -> BuiltInModelType:
    if not BuiltInModelType.is_built_in_model(model_type):
        raise ValueError(f"Invalid built-in model type: {model_type}")
    return BuiltInModelType(model_type)


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


class ModelInfo:
    def __init__(
        self,
        model_id: str,
        model_type: str,
        category: ModelCategory,
        state: ModelStates,
    ):
        self.model_id = model_id
        self.model_type = model_type
        self.category = category
        self.state = state


TIMER_REPO_ID = {
    BuiltInModelType.TIMER_XL: "thuml/timer-base-84m",
    BuiltInModelType.SUNDIAL: "thuml/sundial-base-128m",
}

# Built-in machine learning models, they can be employed directly
BUILT_IN_MACHINE_LEARNING_MODEL_MAP = {
    # forecast models
    "arima": ModelInfo(
        model_id="arima",
        model_type=BuiltInModelType.ARIMA.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    "holtwinters": ModelInfo(
        model_id="holtwinters",
        model_type=BuiltInModelType.HOLTWINTERS.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    "exponential_smoothing": ModelInfo(
        model_id="exponential_smoothing",
        model_type=BuiltInModelType.EXPONENTIAL_SMOOTHING.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    "naive_forecaster": ModelInfo(
        model_id="naive_forecaster",
        model_type=BuiltInModelType.NAIVE_FORECASTER.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    "stl_forecaster": ModelInfo(
        model_id="stl_forecaster",
        model_type=BuiltInModelType.STL_FORECASTER.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    # anomaly detection models
    "gaussian_hmm": ModelInfo(
        model_id="gaussian_hmm",
        model_type=BuiltInModelType.GAUSSIAN_HMM.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    "gmm_hmm": ModelInfo(
        model_id="gmm_hmm",
        model_type=BuiltInModelType.GMM_HMM.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
    "stray": ModelInfo(
        model_id="stray",
        model_type=BuiltInModelType.STRAY.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.ACTIVE,
    ),
}

# Built-in large time series models (LTSM), their weights are not included in AINode by default
BUILT_IN_LTSM_MAP = {
    "timer_xl": ModelInfo(
        model_id="timer_xl",
        model_type=BuiltInModelType.TIMER_XL.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.LOADING,
    ),
    "sundial": ModelInfo(
        model_id="sundial",
        model_type=BuiltInModelType.SUNDIAL.value,
        category=ModelCategory.BUILT_IN,
        state=ModelStates.LOADING,
    ),
}
