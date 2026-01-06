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

from typing import Dict, Optional

from iotdb.ainode.core.model.model_constants import ModelCategory, ModelStates


class ModelInfo:
    def __init__(
        self,
        model_id: str,
        category: ModelCategory,
        state: ModelStates,
        model_type: str = "",
        pipeline_cls: str = "",
        repo_id: str = "",
        auto_map: Optional[Dict] = None,
        transformers_registered: bool = False,
    ):
        self.model_id = model_id
        self.model_type = model_type
        self.category = category
        self.state = state
        self.pipeline_cls = pipeline_cls
        self.repo_id = repo_id
        self.auto_map = auto_map  # If exists, indicates it's a Transformers model
        self.transformers_registered = (
            transformers_registered  # Internal flag: whether registered to Transformers
        )

    def __repr__(self):
        return (
            f"ModelInfo(model_id={self.model_id}, model_type={self.model_type}, "
            f"category={self.category.value}, state={self.state.value}, "
            f"has_auto_map={self.auto_map is not None})"
        )


BUILTIN_SKTIME_MODEL_MAP = {
    # forecast models
    "arima": ModelInfo(
        model_id="arima",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    "holtwinters": ModelInfo(
        model_id="holtwinters",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    "exponential_smoothing": ModelInfo(
        model_id="exponential_smoothing",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    "naive_forecaster": ModelInfo(
        model_id="naive_forecaster",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    "stl_forecaster": ModelInfo(
        model_id="stl_forecaster",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    # anomaly detection models
    "gaussian_hmm": ModelInfo(
        model_id="gaussian_hmm",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    "gmm_hmm": ModelInfo(
        model_id="gmm_hmm",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
    "stray": ModelInfo(
        model_id="stray",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sktime",
    ),
}

# Built-in huggingface transformers models, their weights are not included in AINode by default
BUILTIN_HF_TRANSFORMERS_MODEL_MAP = {
    "timer_xl": ModelInfo(
        model_id="timer_xl",
        category=ModelCategory.BUILTIN,
        state=ModelStates.INACTIVE,
        model_type="timer",
        pipeline_cls="pipeline_timer.TimerPipeline",
        repo_id="thuml/timer-base-84m",
        auto_map={
            "AutoConfig": "configuration_timer.TimerConfig",
            "AutoModelForCausalLM": "modeling_timer.TimerForPrediction",
        },
        transformers_registered=True,
    ),
    "sundial": ModelInfo(
        model_id="sundial",
        category=ModelCategory.BUILTIN,
        state=ModelStates.INACTIVE,
        model_type="sundial",
        pipeline_cls="pipeline_sundial.SundialPipeline",
        repo_id="thuml/sundial-base-128m",
        auto_map={
            "AutoConfig": "configuration_sundial.SundialConfig",
            "AutoModelForCausalLM": "modeling_sundial.SundialForPrediction",
        },
        transformers_registered=True,
    ),
    "chronos2": ModelInfo(
        model_id="chronos2",
        category=ModelCategory.BUILTIN,
        state=ModelStates.INACTIVE,
        model_type="t5",
        pipeline_cls="pipeline_chronos2.Chronos2Pipeline",
        repo_id="amazon/chronos-2",
        auto_map={
            "AutoConfig": "config.Chronos2CoreConfig",
            "AutoModelForCausalLM": "model.Chronos2Model",
        },
    ),
}
