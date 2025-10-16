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
1import inspect
1import logging
1import os
1from enum import Enum
1from typing import List
1
1from iotdb.ainode.core.model.model_enums import BuiltInModelType
1from iotdb.thrift.common.ttypes import TEndPoint
1
1AINODE_VERSION_INFO = "UNKNOWN"
1AINODE_BUILD_INFO = "UNKNOWN"
1AINODE_CONF_DIRECTORY_NAME = "conf"
1AINODE_ROOT_CONF_DIRECTORY_NAME = "conf"
1AINODE_CONF_FILE_NAME = "iotdb-ainode.properties"
1AINODE_CONF_GIT_FILE_NAME = "git.properties"
1AINODE_CONF_POM_FILE_NAME = "pom.properties"
1AINODE_SYSTEM_FILE_NAME = "system.properties"
1
1# AINode cluster configuration
1AINODE_CLUSTER_NAME = "defaultCluster"
1AINODE_TARGET_CONFIG_NODE_LIST = TEndPoint("127.0.0.1", 10710)
1AINODE_RPC_ADDRESS = "127.0.0.1"
1AINODE_RPC_PORT = 10810
1AINODE_CLUSTER_INGRESS_ADDRESS = "127.0.0.1"
1AINODE_CLUSTER_INGRESS_PORT = 6667
1AINODE_CLUSTER_INGRESS_USERNAME = "root"
1AINODE_CLUSTER_INGRESS_PASSWORD = "root"
1AINODE_CLUSTER_INGRESS_TIME_ZONE = "UTC+8"
1
1# RPC config
1AINODE_THRIFT_COMPRESSION_ENABLED = False
1DEFAULT_RECONNECT_TIMEOUT = 20
1DEFAULT_RECONNECT_TIMES = 3
1
1# AINode inference configuration
1AINODE_INFERENCE_BATCH_INTERVAL_IN_MS = 15
1AINODE_INFERENCE_MAX_PREDICT_LENGTH = 2880
1AINODE_INFERENCE_MODEL_MEM_USAGE_MAP = {
1    BuiltInModelType.SUNDIAL.value: 1036 * 1024**2,  # 1036 MiB
1    BuiltInModelType.TIMER_XL.value: 856 * 1024**2,  # 856 MiB
1}  # the memory usage of each model in bytes
1AINODE_INFERENCE_MEMORY_USAGE_RATIO = 0.4  # the device space allocated for inference
1AINODE_INFERENCE_EXTRA_MEMORY_RATIO = (
1    1.2  # the overhead ratio for inference, used to estimate the pool size
1)
1
1# AINode folder structure
1AINODE_ROOT_DIR = os.path.dirname(
1    os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
1)
1AINODE_MODELS_DIR = "data/ainode/models"
1AINODE_BUILTIN_MODELS_DIR = "data/ainode/models/weights"  # For built-in models, we only need to store their weights and config.
1AINODE_SYSTEM_DIR = "data/ainode/system"
1AINODE_LOG_DIR = "logs"
1
1# AINode log
1LOG_FILE_TYPE = ["all", "info", "warn", "error"]
1AINODE_LOG_FILE_NAME_PREFIX = "log_ainode_"
1AINODE_LOG_FILE_LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
1DEFAULT_LOG_LEVEL = logging.INFO
1INFERENCE_LOG_FILE_NAME_PREFIX_TEMPLATE = (
1    "log_inference_rank_{}_"  # example: log_inference_rank_0_all.log
1)
1
1# AINode model management
1MODEL_WEIGHTS_FILE_IN_SAFETENSORS = "model.safetensors"
1MODEL_CONFIG_FILE_IN_JSON = "config.json"
1MODEL_WEIGHTS_FILE_IN_PT = "model.pt"
1MODEL_CONFIG_FILE_IN_YAML = "config.yaml"
1DEFAULT_CHUNK_SIZE = 8192
1
1
1class TSStatusCode(Enum):
1    SUCCESS_STATUS = 200
1    REDIRECTION_RECOMMEND = 400
1    MODEL_EXIST_ERROR = 1502
1    MODEL_NOT_FOUND_ERROR = 1505
1    UNAVAILABLE_AI_DEVICE_ERROR = 1507
1    AINODE_INTERNAL_ERROR = 1510
1    INVALID_URI_ERROR = 1511
1    INVALID_INFERENCE_CONFIG = 1512
1    INFERENCE_INTERNAL_ERROR = 1520
1
1    def get_status_code(self) -> int:
1        return self.value
1
1
1class TaskType(Enum):
1    FORECAST = "forecast"
1
1
1class OptionsKey(Enum):
1    # common
1    TASK_TYPE = "task_type"
1    MODEL_TYPE = "model_type"
1    AUTO_TUNING = "auto_tuning"
1    INPUT_VARS = "input_vars"
1
1    # forecast
1    INPUT_LENGTH = "input_length"
1    PREDICT_LENGTH = "predict_length"
1    PREDICT_INDEX_LIST = "predict_index_list"
1    INPUT_TYPE_LIST = "input_type_list"
1
1    def name(self) -> str:
1        return self.value
1
1
1class HyperparameterName(Enum):
1    # Training hyperparameter
1    LEARNING_RATE = "learning_rate"
1    EPOCHS = "epochs"
1    BATCH_SIZE = "batch_size"
1    USE_GPU = "use_gpu"
1    NUM_WORKERS = "num_workers"
1
1    # Structure hyperparameter
1    KERNEL_SIZE = "kernel_size"
1    INPUT_VARS = "input_vars"
1    BLOCK_TYPE = "block_type"
1    D_MODEL = "d_model"
1    INNER_LAYERS = "inner_layer"
1    OUTER_LAYERS = "outer_layer"
1
1    def name(self):
1        return self.value
1
1
1class ForecastModelType(Enum):
1    DLINEAR = "dlinear"
1    DLINEAR_INDIVIDUAL = "dlinear_individual"
1    NBEATS = "nbeats"
1
1    @classmethod
1    def values(cls) -> List[str]:
1        values = []
1        for item in list(cls):
1            values.append(item.value)
1        return values
1
1
1class ModelInputName(Enum):
1    DATA_X = "data_x"
1    TIME_STAMP_X = "time_stamp_x"
1    TIME_STAMP_Y = "time_stamp_y"
1    DEC_INP = "dec_inp"
1
1
1class AttributeName(Enum):
1    # forecast Attribute
1    PREDICT_LENGTH = "predict_length"
1
1    # NaiveForecaster
1    STRATEGY = "strategy"
1    SP = "sp"
1
1    # STLForecaster
1    # SP = 'sp'
1    SEASONAL = "seasonal"
1    SEASONAL_DEG = "seasonal_deg"
1    TREND_DEG = "trend_deg"
1    LOW_PASS_DEG = "low_pass_deg"
1    SEASONAL_JUMP = "seasonal_jump"
1    TREND_JUMP = "trend_jump"
1    LOSS_PASS_JUMP = "low_pass_jump"
1
1    # ExponentialSmoothing
1    DAMPED_TREND = "damped_trend"
1    INITIALIZATION_METHOD = "initialization_method"
1    OPTIMIZED = "optimized"
1    REMOVE_BIAS = "remove_bias"
1    USE_BRUTE = "use_brute"
1
1    # Arima
1    ORDER = "order"
1    SEASONAL_ORDER = "seasonal_order"
1    METHOD = "method"
1    MAXITER = "maxiter"
1    SUPPRESS_WARNINGS = "suppress_warnings"
1    OUT_OF_SAMPLE_SIZE = "out_of_sample_size"
1    SCORING = "scoring"
1    WITH_INTERCEPT = "with_intercept"
1    TIME_VARYING_REGRESSION = "time_varying_regression"
1    ENFORCE_STATIONARITY = "enforce_stationarity"
1    ENFORCE_INVERTIBILITY = "enforce_invertibility"
1    SIMPLE_DIFFERENCING = "simple_differencing"
1    MEASUREMENT_ERROR = "measurement_error"
1    MLE_REGRESSION = "mle_regression"
1    HAMILTON_REPRESENTATION = "hamilton_representation"
1    CONCENTRATE_SCALE = "concentrate_scale"
1
1    # GAUSSIAN_HMM
1    N_COMPONENTS = "n_components"
1    COVARIANCE_TYPE = "covariance_type"
1    MIN_COVAR = "min_covar"
1    STARTPROB_PRIOR = "startprob_prior"
1    TRANSMAT_PRIOR = "transmat_prior"
1    MEANS_PRIOR = "means_prior"
1    MEANS_WEIGHT = "means_weight"
1    COVARS_PRIOR = "covars_prior"
1    COVARS_WEIGHT = "covars_weight"
1    ALGORITHM = "algorithm"
1    N_ITER = "n_iter"
1    TOL = "tol"
1    PARAMS = "params"
1    INIT_PARAMS = "init_params"
1    IMPLEMENTATION = "implementation"
1
1    # GMMHMM
1    # N_COMPONENTS = "n_components"
1    N_MIX = "n_mix"
1    # MIN_COVAR = "min_covar"
1    # STARTPROB_PRIOR = "startprob_prior"
1    # TRANSMAT_PRIOR = "transmat_prior"
1    WEIGHTS_PRIOR = "weights_prior"
1
1    # MEANS_PRIOR = "means_prior"
1    # MEANS_WEIGHT = "means_weight"
1    # ALGORITHM = "algorithm"
1    # COVARIANCE_TYPE = "covariance_type"
1    # N_ITER = "n_iter"
1    # TOL = "tol"
1    # INIT_PARAMS = "init_params"
1    # PARAMS = "params"
1    # IMPLEMENTATION = "implementation"
1
1    # STRAY
1    ALPHA = "alpha"
1    K = "k"
1    KNN_ALGORITHM = "knn_algorithm"
1    P = "p"
1    SIZE_THRESHOLD = "size_threshold"
1    OUTLIER_TAIL = "outlier_tail"
1
1    # timerxl
1    INPUT_TOKEN_LEN = "input_token_len"
1    HIDDEN_SIZE = "hidden_size"
1    INTERMEDIATE_SIZE = "intermediate_size"
1    OUTPUT_TOKEN_LENS = "output_token_lens"
1    NUM_HIDDEN_LAYERS = "num_hidden_layers"
1    NUM_ATTENTION_HEADS = "num_attention_heads"
1    HIDDEN_ACT = "hidden_act"
1    USE_CACHE = "use_cache"
1    ROPE_THETA = "rope_theta"
1    ATTENTION_DROPOUT = "attention_dropout"
1    INITIALIZER_RANGE = "initializer_range"
1    MAX_POSITION_EMBEDDINGS = "max_position_embeddings"
1    CKPT_PATH = "ckpt_path"
1
1    # sundial
1    DROPOUT_RATE = "dropout_rate"
1    FLOW_LOSS_DEPTH = "flow_loss_depth"
1    NUM_SAMPLING_STEPS = "num_sampling_steps"
1    DIFFUSION_BATCH_MUL = "diffusion_batch_mul"
1
1    def name(self) -> str:
1        return self.value
1