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
import glob
import os

from iotdb.ainode.core.constant import (
    MODEL_CONFIG_FILE_IN_JSON,
    MODEL_CONFIG_FILE_IN_YAML,
    MODEL_WEIGHTS_FILE_IN_PT,
    MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
)
from iotdb.ainode.core.model.model_enums import (
    BuiltInModelType,
    ModelCategory,
    ModelFileType,
    ModelStates,
)


def get_model_file_type(model_path: str) -> ModelFileType:
    """
    Determine the file type of the specified model directory.
    """
    if _has_safetensors_format(model_path):
        return ModelFileType.SAFETENSORS
    elif _has_pytorch_format(model_path):
        return ModelFileType.PYTORCH
    else:
        return ModelFileType.UNKNOWN


def _has_safetensors_format(path: str) -> bool:
    """Check if directory contains safetensors files."""
    safetensors_files = glob.glob(os.path.join(path, MODEL_WEIGHTS_FILE_IN_SAFETENSORS))
    json_files = glob.glob(os.path.join(path, MODEL_CONFIG_FILE_IN_JSON))
    return len(safetensors_files) > 0 and len(json_files) > 0


def _has_pytorch_format(path: str) -> bool:
    """Check if directory contains pytorch files."""
    pt_files = glob.glob(os.path.join(path, MODEL_WEIGHTS_FILE_IN_PT))
    yaml_files = glob.glob(os.path.join(path, MODEL_CONFIG_FILE_IN_YAML))
    return len(pt_files) > 0 and len(yaml_files) > 0


def get_built_in_model_type(model_type: str) -> BuiltInModelType:
    if not BuiltInModelType.is_built_in_model(model_type):
        raise ValueError(f"Invalid built-in model type: {model_type}")
    return BuiltInModelType(model_type)


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
