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

import logging
import os
from enum import Enum
from typing import List

from iotdb.ainode.core.model.model_enums import BuiltInModelType
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
AINODE_CLUSTER_INGRESS_PASSWORD = "TimechoDB@2021"
AINODE_CLUSTER_INGRESS_TIME_ZONE = "UTC+8"

# RPC config
AINODE_THRIFT_COMPRESSION_ENABLED = False
DEFAULT_RECONNECT_TIMEOUT = 20
DEFAULT_RECONNECT_TIMES = 3

# AINode inference configuration
AINODE_INFERENCE_BATCH_INTERVAL_IN_MS = 15
AINODE_INFERENCE_MAX_PREDICT_LENGTH = 2880
AINODE_INFERENCE_MODEL_MEM_USAGE_MAP = {
    BuiltInModelType.SUNDIAL.value: 1036 * 1024**2,  # 1036 MiB
    BuiltInModelType.TIMER_XL.value: 856 * 1024**2,  # 856 MiB
}  # the memory usage of each model in bytes
AINODE_INFERENCE_MEMORY_USAGE_RATIO = 0.4  # the device space allocated for inference
AINODE_INFERENCE_EXTRA_MEMORY_RATIO = (
    1.2  # the overhead ratio for inference, used to estimate the pool size
)

# AINode folder structure
AINODE_MODELS_DIR = os.path.join(IOTDB_AINODE_HOME, "data/ainode/models")
AINODE_BUILTIN_MODELS_DIR = os.path.join(
    IOTDB_AINODE_HOME, "data/ainode/models/weights"
)  # For built-in models, we only need to store their weights and config.
AINODE_SYSTEM_DIR = os.path.join(IOTDB_AINODE_HOME, "data/ainode/system")
AINODE_LOG_DIR = os.path.join(IOTDB_AINODE_HOME, "logs")

# AINode log
LOG_FILE_TYPE = ["all", "info", "warn", "error"]
AINODE_LOG_FILE_NAME_PREFIX = "log_ainode_"
AINODE_LOG_FILE_LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
DEFAULT_LOG_LEVEL = logging.INFO
INFERENCE_LOG_FILE_NAME_PREFIX_TEMPLATE = (
    "log_inference_rank_{}_"  # example: log_inference_rank_0_all.log
)
TRAINING_LOG_FILE_NAME_PREFIX_TEMPLATE = (
    "log_training_rank_{}_"  # example: log_training_rank_0_all.log
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

    # Training status codes
    INVALID_TRAINING_CONFIG = 1550
    TRAINING_INTERNAL_ERROR = 1551

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


class AttributeName(Enum):
    # forecast Attribute
    PREDICT_LENGTH = "predict_length"

    # NaiveForecaster
    STRATEGY = "strategy"
    SP = "sp"

    # STLForecaster
    # SP = 'sp'
    SEASONAL = "seasonal"
    SEASONAL_DEG = "seasonal_deg"
    TREND_DEG = "trend_deg"
    LOW_PASS_DEG = "low_pass_deg"
    SEASONAL_JUMP = "seasonal_jump"
    TREND_JUMP = "trend_jump"
    LOSS_PASS_JUMP = "low_pass_jump"

    # ExponentialSmoothing
    DAMPED_TREND = "damped_trend"
    INITIALIZATION_METHOD = "initialization_method"
    OPTIMIZED = "optimized"
    REMOVE_BIAS = "remove_bias"
    USE_BRUTE = "use_brute"

    # Arima
    ORDER = "order"
    SEASONAL_ORDER = "seasonal_order"
    METHOD = "method"
    MAXITER = "maxiter"
    SUPPRESS_WARNINGS = "suppress_warnings"
    OUT_OF_SAMPLE_SIZE = "out_of_sample_size"
    SCORING = "scoring"
    WITH_INTERCEPT = "with_intercept"
    TIME_VARYING_REGRESSION = "time_varying_regression"
    ENFORCE_STATIONARITY = "enforce_stationarity"
    ENFORCE_INVERTIBILITY = "enforce_invertibility"
    SIMPLE_DIFFERENCING = "simple_differencing"
    MEASUREMENT_ERROR = "measurement_error"
    MLE_REGRESSION = "mle_regression"
    HAMILTON_REPRESENTATION = "hamilton_representation"
    CONCENTRATE_SCALE = "concentrate_scale"

    # GAUSSIAN_HMM
    N_COMPONENTS = "n_components"
    COVARIANCE_TYPE = "covariance_type"
    MIN_COVAR = "min_covar"
    STARTPROB_PRIOR = "startprob_prior"
    TRANSMAT_PRIOR = "transmat_prior"
    MEANS_PRIOR = "means_prior"
    MEANS_WEIGHT = "means_weight"
    COVARS_PRIOR = "covars_prior"
    COVARS_WEIGHT = "covars_weight"
    ALGORITHM = "algorithm"
    N_ITER = "n_iter"
    TOL = "tol"
    PARAMS = "params"
    INIT_PARAMS = "init_params"
    IMPLEMENTATION = "implementation"

    # GMMHMM
    # N_COMPONENTS = "n_components"
    N_MIX = "n_mix"
    # MIN_COVAR = "min_covar"
    # STARTPROB_PRIOR = "startprob_prior"
    # TRANSMAT_PRIOR = "transmat_prior"
    WEIGHTS_PRIOR = "weights_prior"

    # MEANS_PRIOR = "means_prior"
    # MEANS_WEIGHT = "means_weight"
    # ALGORITHM = "algorithm"
    # COVARIANCE_TYPE = "covariance_type"
    # N_ITER = "n_iter"
    # TOL = "tol"
    # INIT_PARAMS = "init_params"
    # PARAMS = "params"
    # IMPLEMENTATION = "implementation"

    # STRAY
    ALPHA = "alpha"
    K = "k"
    KNN_ALGORITHM = "knn_algorithm"
    P = "p"
    SIZE_THRESHOLD = "size_threshold"
    OUTLIER_TAIL = "outlier_tail"

    # timerxl
    INPUT_TOKEN_LEN = "input_token_len"
    HIDDEN_SIZE = "hidden_size"
    INTERMEDIATE_SIZE = "intermediate_size"
    OUTPUT_TOKEN_LENS = "output_token_lens"
    NUM_HIDDEN_LAYERS = "num_hidden_layers"
    NUM_ATTENTION_HEADS = "num_attention_heads"
    HIDDEN_ACT = "hidden_act"
    USE_CACHE = "use_cache"
    ROPE_THETA = "rope_theta"
    ATTENTION_DROPOUT = "attention_dropout"
    INITIALIZER_RANGE = "initializer_range"
    MAX_POSITION_EMBEDDINGS = "max_position_embeddings"
    CKPT_PATH = "ckpt_path"

    # sundial
    DROPOUT_RATE = "dropout_rate"
    FLOW_LOSS_DEPTH = "flow_loss_depth"
    NUM_SAMPLING_STEPS = "num_sampling_steps"
    DIFFUSION_BATCH_MUL = "diffusion_batch_mul"

    def name(self) -> str:
        return self.value
