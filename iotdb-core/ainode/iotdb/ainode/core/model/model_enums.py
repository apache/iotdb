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
1from enum import Enum
1from typing import List
1
1
1class BuiltInModelType(Enum):
1    # forecast models
1    ARIMA = "Arima"
1    HOLTWINTERS = "HoltWinters"
1    EXPONENTIAL_SMOOTHING = "ExponentialSmoothing"
1    NAIVE_FORECASTER = "NaiveForecaster"
1    STL_FORECASTER = "StlForecaster"
1
1    # anomaly detection models
1    GAUSSIAN_HMM = "GaussianHmm"
1    GMM_HMM = "GmmHmm"
1    STRAY = "Stray"
1
1    # large time series models (LTSM)
1    TIMER_XL = "Timer-XL"
1    # sundial
1    SUNDIAL = "Timer-Sundial"
1
1    @classmethod
1    def values(cls) -> List[str]:
1        return [item.value for item in cls]
1
1    @staticmethod
1    def is_built_in_model(model_type: str) -> bool:
1        """
1        Check if the given model type corresponds to a built-in model.
1        """
1        return model_type in BuiltInModelType.values()
1
1
1class ModelFileType(Enum):
1    SAFETENSORS = "safetensors"
1    PYTORCH = "pytorch"
1    UNKNOWN = "unknown"
1
1
1class ModelCategory(Enum):
1    BUILT_IN = "BUILT-IN"
1    FINE_TUNED = "FINE-TUNED"
1    USER_DEFINED = "USER-DEFINED"
1
1
1class ModelStates(Enum):
1    ACTIVE = "ACTIVE"
1    INACTIVE = "INACTIVE"
1    LOADING = "LOADING"
1    DROPPING = "DROPPING"
1    TRAINING = "TRAINING"
1    FAILED = "FAILED"
1