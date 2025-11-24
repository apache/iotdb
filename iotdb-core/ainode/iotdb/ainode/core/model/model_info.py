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

from typing import Dict, List, Optional, Tuple

from iotdb.ainode.core.model.model_enums import ModelCategory, ModelStates

# Map for inferring which HuggingFace repository to download from based on model ID
REPO_ID_MAP = {
    "timerxl": "thuml/timer-base-84m",
    "sundial": "thuml/sundial-base-128m",
    # More mappings can be added as needed
}

# Model file constants
MODEL_CONFIG_FILE = "config.json"
MODEL_WEIGHTS_FILE = "model.safetensors"


class ModelInfo:
    def __init__(
        self,
        model_id: str,
        model_type: str,
        category: ModelCategory,
        state: ModelStates,
        path: str = "",
        auto_map: Optional[Dict] = None,
        _transformers_registered: bool = False,
    ):
        self.model_id = model_id
        self.model_type = model_type
        self.category = category
        self.state = state
        self.path = path
        self.auto_map = auto_map  # If exists, indicates it's a Transformers model
        self._transformers_registered = _transformers_registered  # Internal flag: whether registered to Transformers

    def __repr__(self):
        return (
            f"ModelInfo(model_id={self.model_id}, model_type={self.model_type}, "
            f"category={self.category.value}, state={self.state.value}, "
            f"path={self.path}, has_auto_map={self.auto_map is not None})"
        )


BUILTIN_SKTIME_MODEL_MAP = {
    # forecast models
    "arima": ModelInfo(
        model_id="arima",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    "holtwinters": ModelInfo(
        model_id="holtwinters",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    "exponential_smoothing": ModelInfo(
        model_id="exponential_smoothing",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    "naive_forecaster": ModelInfo(
        model_id="naive_forecaster",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    "stl_forecaster": ModelInfo(
        model_id="stl_forecaster",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    # anomaly detection models
    "gaussian_hmm": ModelInfo(
        model_id="gaussian_hmm",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    "gmm_hmm": ModelInfo(
        model_id="gmm_hmm",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
    "stray": ModelInfo(
        model_id="stray",
        model_type="sktime",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
    ),
}

# Built-in huggingface transformers models, their weights are not included in AINode by default
BUILTIN_HF_TRANSFORMERS_MODEL_MAP = {
    "timerxl": ModelInfo(
        model_id="timerxl",
        model_type="timer",
        category=ModelCategory.BUILTIN,
        state=ModelStates.INACTIVE,
    ),
    "sundial": ModelInfo(
        model_id="sundial",
        model_type="sundial",
        category=ModelCategory.BUILTIN,
        state=ModelStates.INACTIVE,
    ),
}
