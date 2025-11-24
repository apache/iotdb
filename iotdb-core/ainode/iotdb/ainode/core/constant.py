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
import inspect
import logging
import os
from enum import Enum
from typing import List

from iotdb.thrift.common.ttypes import TEndPoint

IOTDB_AINODE_HOME = os.getenv("IOTDB_AINODE_HOME", "")
AINODE_VERSION_INFO = "UNKNOWN"
AINODE_BUILD_INFO = "UNKNOWN"
AINODE_CONF_DIRECTORY_NAME = os.path.join(IOTDB_AINODE_HOME, "conf")
AINODE_CONF_FILE_NAME = "iotdb-ainode.properties"
AINODE_CONF_GIT_FILE_NAME = "git.properties"
AINODE_CONF_POM_FILE_NAME = "pom.properties"
AINODE_SYSTEM_FILE_NAME = "system.properties"

# AINode cluster configuration
AINODE_CLUSTER_NAME = "defaultCluster"
AINODE_TARGET_CONFIG_NODE_LIST = TEndPoint("127.0.0.1", 10710)
AINODE_RPC_ADDRESS = "127.0.0.1"
AINODE_RPC_PORT = 10810
AINODE_CLUSTER_INGRESS_ADDRESS = "127.0.0.1"
AINODE_CLUSTER_INGRESS_PORT = 6667
AINODE_CLUSTER_INGRESS_USERNAME = "root"
AINODE_CLUSTER_INGRESS_PASSWORD = "root"
AINODE_CLUSTER_INGRESS_TIME_ZONE = "UTC+8"

# RPC config
AINODE_THRIFT_COMPRESSION_ENABLED = False
DEFAULT_RECONNECT_TIMEOUT = 20
DEFAULT_RECONNECT_TIMES = 3

# AINode inference configuration
AINODE_INFERENCE_BATCH_INTERVAL_IN_MS = 15
AINODE_INFERENCE_MAX_PREDICT_LENGTH = 2880
AINODE_INFERENCE_MODEL_MEM_USAGE_MAP = {
    "sundial": 1036 * 1024**2,  # 1036 MiB
    "timerxl": 856 * 1024**2,  # 856 MiB
}  # the memory usage of each model in bytes
AINODE_INFERENCE_MEMORY_USAGE_RATIO = 0.4  # the device space allocated for inference
AINODE_INFERENCE_EXTRA_MEMORY_RATIO = (
    1.2  # the overhead ratio for inference, used to estimate the pool size
)

AINODE_MODELS_DIR = os.path.join(IOTDB_AINODE_HOME, "data/ainode/models")
AINODE_BUILTIN_MODELS_DIR = os.path.join(
    IOTDB_AINODE_HOME, "data/ainode/models/builtin"
)  # For built-in models, we only need to store their weights and config.
AINODE_FINETUNE_MODELS_DIR = os.path.join(
    IOTDB_AINODE_HOME, "data/ainode/models/finetune"
)
AINODE_USER_DEFINED_MODELS_DIR = os.path.join(
    IOTDB_AINODE_HOME, "data/ainode/models/user_defined"
)
AINODE_SYSTEM_DIR = "data/ainode/system"
AINODE_LOG_DIR = "logs"
AINODE_CACHE_DIR = os.path.expanduser("~/.cache/ainode")

# AINode log
LOG_FILE_TYPE = ["all", "info", "warn", "error"]
AINODE_LOG_FILE_NAME_PREFIX = "log_ainode_"
AINODE_LOG_FILE_LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
DEFAULT_LOG_LEVEL = logging.INFO
INFERENCE_LOG_FILE_NAME_PREFIX_TEMPLATE = (
    "log_inference_rank_{}_"  # example: log_inference_rank_0_all.log
)

# AINode model management
MODEL_WEIGHTS_FILE_IN_SAFETENSORS = "model.safetensors"
MODEL_CONFIG_FILE_IN_JSON = "config.json"
MODEL_WEIGHTS_FILE_IN_PT = "model.pt"
MODEL_CONFIG_FILE_IN_YAML = "config.yaml"
DEFAULT_CHUNK_SIZE = 8192


class TSStatusCode(Enum):
    SUCCESS_STATUS = 200
    REDIRECTION_RECOMMEND = 400
    MODEL_EXIST_ERROR = 1502
    MODEL_NOT_FOUND_ERROR = 1505
    UNAVAILABLE_AI_DEVICE_ERROR = 1507
    AINODE_INTERNAL_ERROR = 1510
    INVALID_URI_ERROR = 1511
    INVALID_INFERENCE_CONFIG = 1512
    INFERENCE_INTERNAL_ERROR = 1520

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


class ModelInputName(Enum):
    DATA_X = "data_x"
    TIME_STAMP_X = "time_stamp_x"
    TIME_STAMP_Y = "time_stamp_y"
    DEC_INP = "dec_inp"
