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

MLNODE_CONF_DIRECTORY_NAME = "resources/conf"
MLNODE_CONF_FILE_NAME = "iotdb-mlnode.toml"
MLNODE_LOG_CONF_FILE_NAME = "logging_config.ini"

MLNODE_MODEL_STORAGE_DIRECTORY_NAME = "models"

TRIAL_ID_PREFIX = "__trial_"
DEFAULT_TRIAL_ID = TRIAL_ID_PREFIX + "0"


class TSStatusCode(Enum):
    SUCCESS_STATUS = 200
    REDIRECTION_RECOMMEND = 400
    MLNODE_INTERNAL_ERROR = 1510

    def get_status_code(self) -> int:
        return self.value


class TaskType(Enum):
    FORECAST = "forecast"


class OptionsKey(Enum):
    # common
    TASK_TYPE = "task_type"
    MODEL_TYPE = "model_type"
    AUTO_TUNING = "auto_tuning"
    INPUT_VARS = "input_vars"

    # forecast
    INPUT_LENGTH = "input_length"
    PREDICT_LENGTH = "predict_length"
    PREDICT_INDEX_LIST = "predict_index_list"
    INPUT_TYPE_LIST = "input_type_list"

    def name(self) -> str:
        return self.value


class HyperparameterName(Enum):
    # Training hyperparameter
    LEARNING_RATE = "learning_rate"
    EPOCHS = "epochs"
    BATCH_SIZE = "batch_size"
    USE_GPU = "use_gpu"
    NUM_WORKERS = "num_workers"

    # Structure hyperparameter
    KERNEL_SIZE = "kernel_size"
    INPUT_VARS = "input_vars"
    BLOCK_TYPE = "block_type"
    D_MODEL = "d_model"
    INNER_LAYERS = "inner_layer"
    OUTER_LAYERS = "outer_layer"

    def name(self):
        return self.value


class ForecastModelType(Enum):
    DLINEAR = "dlinear"
    DLINEAR_INDIVIDUAL = "dlinear_individual"
    NBEATS = "nbeats"

    @classmethod
    def values(cls) -> List[str]:
        values = []
        for item in list(cls):
            values.append(item.value)
        return values


class ModelInputName(Enum):
    DATA_X = "data_x"
    TIME_STAMP_X = "time_stamp_x"
    TIME_STAMP_Y = "time_stamp_y"
    DEC_INP = "dec_inp"

