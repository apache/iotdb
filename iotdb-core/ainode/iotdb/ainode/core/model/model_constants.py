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

# ==================== File Name Constants ====================
#
# All file names used for model persistence are defined here.
# Never hard-code these strings elsewhere – always import from
# this module.

# -- Config files --
CONFIG_JSON = "config.json"
CONFIG_YAML = "config.yaml"
ADAPTER_CONFIG = "adapter_config.json"

# -- Full model weights --
MODEL_SAFETENSORS = "model.safetensors"
MODEL_PT = "model.pt"
MODEL_BIN = "pytorch_model.bin"  # legacy HuggingFace format

# -- Adapter weights (DualWeaver / PEFT / LoRA) --
ADAPTER_SAFETENSORS = "adapter_model.safetensors"
ADAPTER_PT = "adapter_model.pt"
ADAPTER_BIN = "adapter_model.bin"

# -- Training state --
TRAINING_STATE = "training_state.pt"

# -- Ordered tuples for detection / searching --
MODEL_WEIGHT_FILES = (MODEL_SAFETENSORS, MODEL_PT, MODEL_BIN)
ADAPTER_WEIGHT_FILES = (ADAPTER_SAFETENSORS, ADAPTER_PT, ADAPTER_BIN)

# -- Backward-compatible aliases (deprecated, will be removed) --
MODEL_WEIGHTS_FILE_IN_SAFETENSORS = MODEL_SAFETENSORS
MODEL_CONFIG_FILE_IN_JSON = CONFIG_JSON
MODEL_WEIGHTS_FILE_IN_PT = MODEL_PT
MODEL_CONFIG_FILE_IN_YAML = CONFIG_YAML


# ==================== Enumerations ====================


class ModelCategory(Enum):
    BUILTIN = "builtin"
    USER_DEFINED = "user_defined"
    FINE_TUNED = "fine_tuned"


class ModelStates(Enum):
    INACTIVE = "inactive"
    ACTIVATING = "activating"
    ACTIVE = "active"
    DROPPING = "dropping"
    TRAINING = "training"
    FAILED = "failed"


class UriType(Enum):
    REPO = "repo"
    FILE = "file"
