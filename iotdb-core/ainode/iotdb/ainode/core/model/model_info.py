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
1import glob
1import os
1
1from iotdb.ainode.core.constant import (
1    MODEL_CONFIG_FILE_IN_JSON,
1    MODEL_CONFIG_FILE_IN_YAML,
1    MODEL_WEIGHTS_FILE_IN_PT,
1    MODEL_WEIGHTS_FILE_IN_SAFETENSORS,
1)
1from iotdb.ainode.core.model.model_enums import (
1    BuiltInModelType,
1    ModelCategory,
1    ModelFileType,
1    ModelStates,
1)
1
1
1def get_model_file_type(model_path: str) -> ModelFileType:
1    """
1    Determine the file type of the specified model directory.
1    """
1    if _has_safetensors_format(model_path):
1        return ModelFileType.SAFETENSORS
1    elif _has_pytorch_format(model_path):
1        return ModelFileType.PYTORCH
1    else:
1        return ModelFileType.UNKNOWN
1
1
1def _has_safetensors_format(path: str) -> bool:
1    """Check if directory contains safetensors files."""
1    safetensors_files = glob.glob(os.path.join(path, MODEL_WEIGHTS_FILE_IN_SAFETENSORS))
1    json_files = glob.glob(os.path.join(path, MODEL_CONFIG_FILE_IN_JSON))
1    return len(safetensors_files) > 0 and len(json_files) > 0
1
1
1def _has_pytorch_format(path: str) -> bool:
1    """Check if directory contains pytorch files."""
1    pt_files = glob.glob(os.path.join(path, MODEL_WEIGHTS_FILE_IN_PT))
1    yaml_files = glob.glob(os.path.join(path, MODEL_CONFIG_FILE_IN_YAML))
1    return len(pt_files) > 0 and len(yaml_files) > 0
1
1
1def get_built_in_model_type(model_type: str) -> BuiltInModelType:
1    if not BuiltInModelType.is_built_in_model(model_type):
1        raise ValueError(f"Invalid built-in model type: {model_type}")
1    return BuiltInModelType(model_type)
1
1
1class ModelInfo:
1    def __init__(
1        self,
1        model_id: str,
1        model_type: str,
1        category: ModelCategory,
1        state: ModelStates,
1    ):
1        self.model_id = model_id
1        self.model_type = model_type
1        self.category = category
1        self.state = state
1
1
1TIMER_REPO_ID = {
1    BuiltInModelType.TIMER_XL: "thuml/timer-base-84m",
1    BuiltInModelType.SUNDIAL: "thuml/sundial-base-128m",
1}
1
1# Built-in machine learning models, they can be employed directly
1BUILT_IN_MACHINE_LEARNING_MODEL_MAP = {
1    # forecast models
1    "arima": ModelInfo(
1        model_id="arima",
1        model_type=BuiltInModelType.ARIMA.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    "holtwinters": ModelInfo(
1        model_id="holtwinters",
1        model_type=BuiltInModelType.HOLTWINTERS.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    "exponential_smoothing": ModelInfo(
1        model_id="exponential_smoothing",
1        model_type=BuiltInModelType.EXPONENTIAL_SMOOTHING.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    "naive_forecaster": ModelInfo(
1        model_id="naive_forecaster",
1        model_type=BuiltInModelType.NAIVE_FORECASTER.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    "stl_forecaster": ModelInfo(
1        model_id="stl_forecaster",
1        model_type=BuiltInModelType.STL_FORECASTER.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    # anomaly detection models
1    "gaussian_hmm": ModelInfo(
1        model_id="gaussian_hmm",
1        model_type=BuiltInModelType.GAUSSIAN_HMM.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    "gmm_hmm": ModelInfo(
1        model_id="gmm_hmm",
1        model_type=BuiltInModelType.GMM_HMM.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1    "stray": ModelInfo(
1        model_id="stray",
1        model_type=BuiltInModelType.STRAY.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.ACTIVE,
1    ),
1}
1
1# Built-in large time series models (LTSM), their weights are not included in AINode by default
1BUILT_IN_LTSM_MAP = {
1    "timer_xl": ModelInfo(
1        model_id="timer_xl",
1        model_type=BuiltInModelType.TIMER_XL.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.LOADING,
1    ),
1    "sundial": ModelInfo(
1        model_id="sundial",
1        model_type=BuiltInModelType.SUNDIAL.value,
1        category=ModelCategory.BUILT_IN,
1        state=ModelStates.LOADING,
1    ),
1}
1