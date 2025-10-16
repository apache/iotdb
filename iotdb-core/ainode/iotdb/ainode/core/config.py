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
1import os
1
1from iotdb.ainode.core.constant import (
1    AINODE_BUILD_INFO,
1    AINODE_BUILTIN_MODELS_DIR,
1    AINODE_CLUSTER_INGRESS_ADDRESS,
1    AINODE_CLUSTER_INGRESS_PASSWORD,
1    AINODE_CLUSTER_INGRESS_PORT,
1    AINODE_CLUSTER_INGRESS_TIME_ZONE,
1    AINODE_CLUSTER_INGRESS_USERNAME,
1    AINODE_CLUSTER_NAME,
1    AINODE_CONF_DIRECTORY_NAME,
1    AINODE_CONF_FILE_NAME,
1    AINODE_CONF_GIT_FILE_NAME,
1    AINODE_CONF_POM_FILE_NAME,
1    AINODE_INFERENCE_BATCH_INTERVAL_IN_MS,
1    AINODE_INFERENCE_EXTRA_MEMORY_RATIO,
1    AINODE_INFERENCE_MAX_PREDICT_LENGTH,
1    AINODE_INFERENCE_MEMORY_USAGE_RATIO,
1    AINODE_INFERENCE_MODEL_MEM_USAGE_MAP,
1    AINODE_LOG_DIR,
1    AINODE_MODELS_DIR,
1    AINODE_ROOT_CONF_DIRECTORY_NAME,
1    AINODE_ROOT_DIR,
1    AINODE_RPC_ADDRESS,
1    AINODE_RPC_PORT,
1    AINODE_SYSTEM_DIR,
1    AINODE_SYSTEM_FILE_NAME,
1    AINODE_TARGET_CONFIG_NODE_LIST,
1    AINODE_THRIFT_COMPRESSION_ENABLED,
1    AINODE_VERSION_INFO,
1)
1from iotdb.ainode.core.exception import BadNodeUrlError
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.util.decorator import singleton
1from iotdb.thrift.common.ttypes import TEndPoint
1
1logger = Logger()
1
1
1class AINodeConfig(object):
1    def __init__(self):
1        self._version_info = AINODE_VERSION_INFO
1        self._build_info = AINODE_BUILD_INFO
1
1        # Cluster configuration
1        self._ainode_id = 0
1        self._cluster_name = AINODE_CLUSTER_NAME
1        self._ain_target_config_node_list: TEndPoint = AINODE_TARGET_CONFIG_NODE_LIST
1        self._ain_rpc_address: str = AINODE_RPC_ADDRESS
1        self._ain_rpc_port: int = AINODE_RPC_PORT
1        self._ain_cluster_ingress_address = AINODE_CLUSTER_INGRESS_ADDRESS
1        self._ain_cluster_ingress_port = AINODE_CLUSTER_INGRESS_PORT
1        self._ain_cluster_ingress_username = AINODE_CLUSTER_INGRESS_USERNAME
1        self._ain_cluster_ingress_password = AINODE_CLUSTER_INGRESS_PASSWORD
1        self._ain_cluster_ingress_time_zone = AINODE_CLUSTER_INGRESS_TIME_ZONE
1
1        # Inference configuration
1        self._ain_inference_batch_interval_in_ms: int = (
1            AINODE_INFERENCE_BATCH_INTERVAL_IN_MS
1        )
1        self._ain_inference_max_predict_length: int = (
1            AINODE_INFERENCE_MAX_PREDICT_LENGTH
1        )
1        self._ain_inference_model_mem_usage_map: dict[str, int] = (
1            AINODE_INFERENCE_MODEL_MEM_USAGE_MAP
1        )
1        self._ain_inference_memory_usage_ratio: float = (
1            AINODE_INFERENCE_MEMORY_USAGE_RATIO
1        )
1        self._ain_inference_extra_memory_ratio: float = (
1            AINODE_INFERENCE_EXTRA_MEMORY_RATIO
1        )
1        # log directory
1        self._ain_logs_dir: str = AINODE_LOG_DIR
1
1        # cache size for ingress dataloader (MB)
1        self._ain_data_cache_size = 50
1
1        # Directory to save models
1        self._ain_models_dir = AINODE_MODELS_DIR
1        self._ain_builtin_models_dir = AINODE_BUILTIN_MODELS_DIR
1        self._ain_system_dir = AINODE_SYSTEM_DIR
1
1        # Whether to enable compression for thrift
1        self._ain_thrift_compression_enabled = AINODE_THRIFT_COMPRESSION_ENABLED
1
1        # use for ssl
1        self._ain_cluster_ingress_ssl_enabled = False
1        self._ain_internal_ssl_enabled = False
1        self._ain_thrift_ssl_cert_file = None
1        self._ain_thrift_ssl_key_file = None
1
1        # Cache number of model storage to avoid repeated loading
1        self._ain_model_storage_cache_size = 30
1
1    def get_cluster_name(self) -> str:
1        return self._cluster_name
1
1    def set_cluster_name(self, cluster_name: str) -> None:
1        self._cluster_name = cluster_name
1
1    def get_version_info(self) -> str:
1        return self._version_info
1
1    def get_ainode_id(self) -> int:
1        return self._ainode_id
1
1    def set_ainode_id(self, ainode_id: int) -> None:
1        self._ainode_id = ainode_id
1
1    def get_build_info(self) -> str:
1        return self._build_info
1
1    def set_build_info(self, build_info: str) -> None:
1        self._build_info = build_info
1
1    def get_ain_data_storage_cache_size(self) -> int:
1        return self._ain_data_cache_size
1
1    def set_ain_data_cache_size(self, ain_data_cache_size: int) -> None:
1        self._ain_data_cache_size = ain_data_cache_size
1
1    def set_version_info(self, version_info: str) -> None:
1        self._version_info = version_info
1
1    def get_ain_rpc_address(self) -> str:
1        return self._ain_rpc_address
1
1    def set_ain_rpc_address(self, ain_rpc_address: str) -> None:
1        self._ain_rpc_address = ain_rpc_address
1
1    def get_ain_rpc_port(self) -> int:
1        return self._ain_rpc_port
1
1    def set_ain_rpc_port(self, ain_rpc_port: int) -> None:
1        self._ain_rpc_port = ain_rpc_port
1
1    def get_ain_inference_batch_interval_in_ms(self) -> int:
1        return self._ain_inference_batch_interval_in_ms
1
1    def set_ain_inference_batch_interval_in_ms(
1        self, ain_inference_batch_interval_in_ms: int
1    ) -> None:
1        self._ain_inference_batch_interval_in_ms = ain_inference_batch_interval_in_ms
1
1    def get_ain_inference_max_predict_length(self) -> int:
1        return self._ain_inference_max_predict_length
1
1    def set_ain_inference_max_predict_length(
1        self, ain_inference_max_predict_length: int
1    ) -> None:
1        self._ain_inference_max_predict_length = ain_inference_max_predict_length
1
1    def get_ain_inference_model_mem_usage_map(self) -> dict[str, int]:
1        return self._ain_inference_model_mem_usage_map
1
1    def set_ain_inference_model_mem_usage_map(
1        self, ain_inference_model_mem_usage_map: dict[str, int]
1    ) -> None:
1        self._ain_inference_model_mem_usage_map = ain_inference_model_mem_usage_map
1
1    def get_ain_inference_memory_usage_ratio(self) -> float:
1        return self._ain_inference_memory_usage_ratio
1
1    def set_ain_inference_memory_usage_ratio(
1        self, ain_inference_memory_usage_ratio: float
1    ) -> None:
1        self._ain_inference_memory_usage_ratio = ain_inference_memory_usage_ratio
1
1    def get_ain_inference_extra_memory_ratio(self) -> float:
1        return self._ain_inference_extra_memory_ratio
1
1    def set_ain_inference_extra_memory_ratio(
1        self, ain_inference_extra_memory_ratio: float
1    ) -> None:
1        self._ain_inference_extra_memory_ratio = ain_inference_extra_memory_ratio
1
1    def get_ain_logs_dir(self) -> str:
1        return self._ain_logs_dir
1
1    def set_ain_logs_dir(self, ain_logs_dir: str) -> None:
1        self._ain_logs_dir = ain_logs_dir
1
1    def get_ain_models_dir(self) -> str:
1        return self._ain_models_dir
1
1    def set_ain_models_dir(self, ain_models_dir: str) -> None:
1        self._ain_models_dir = ain_models_dir
1
1    def get_ain_builtin_models_dir(self) -> str:
1        return self._ain_builtin_models_dir
1
1    def set_ain_builtin_models_dir(self, ain_builtin_models_dir: str) -> None:
1        self._ain_builtin_models_dir = ain_builtin_models_dir
1
1    def get_ain_system_dir(self) -> str:
1        return self._ain_system_dir
1
1    def set_ain_system_dir(self, ain_system_dir: str) -> None:
1        self._ain_system_dir = ain_system_dir
1
1    def get_ain_thrift_compression_enabled(self) -> bool:
1        return self._ain_thrift_compression_enabled
1
1    def set_ain_thrift_compression_enabled(
1        self, ain_thrift_compression_enabled: int
1    ) -> None:
1        self._ain_thrift_compression_enabled = ain_thrift_compression_enabled
1
1    def get_ain_cluster_ingress_ssl_enabled(self) -> bool:
1        return self._ain_cluster_ingress_ssl_enabled
1
1    def set_ain_cluster_ingress_ssl_enabled(
1        self, ain_cluster_ingress_ssl_enabled: int
1    ) -> None:
1        self._ain_cluster_ingress_ssl_enabled = ain_cluster_ingress_ssl_enabled
1
1    def get_ain_internal_ssl_enabled(self) -> bool:
1        return self._ain_internal_ssl_enabled
1
1    def set_ain_internal_ssl_enabled(self, ain_internal_ssl_enabled: int) -> None:
1        self._ain_internal_ssl_enabled = ain_internal_ssl_enabled
1
1    def get_ain_thrift_ssl_cert_file(self) -> str:
1        return self._ain_thrift_ssl_cert_file
1
1    def set_ain_thrift_ssl_cert_file(self, ain_thrift_ssl_cert_file: str) -> None:
1        self._ain_thrift_ssl_cert_file = ain_thrift_ssl_cert_file
1
1    def get_ain_thrift_ssl_key_file(self) -> str:
1        return self._ain_thrift_ssl_key_file
1
1    def set_ain_thrift_ssl_key_file(self, ain_thrift_ssl_key_file: str) -> None:
1        self._ain_thrift_ssl_key_file = ain_thrift_ssl_key_file
1
1    def get_ain_model_storage_cache_size(self) -> int:
1        return self._ain_model_storage_cache_size
1
1    def get_ain_target_config_node_list(self) -> TEndPoint:
1        return self._ain_target_config_node_list
1
1    def set_ain_target_config_node_list(self, ain_target_config_node_list: str) -> None:
1        self._ain_target_config_node_list = parse_endpoint_url(
1            ain_target_config_node_list
1        )
1
1    def get_ain_cluster_ingress_address(self) -> str:
1        return self._ain_cluster_ingress_address
1
1    def set_ain_cluster_ingress_address(self, ain_cluster_ingress_address: str) -> None:
1        self._ain_cluster_ingress_address = ain_cluster_ingress_address
1
1    def get_ain_cluster_ingress_port(self) -> int:
1        return self._ain_cluster_ingress_port
1
1    def set_ain_cluster_ingress_port(self, ain_cluster_ingress_port: int) -> None:
1        self._ain_cluster_ingress_port = ain_cluster_ingress_port
1
1    def get_ain_cluster_ingress_username(self) -> str:
1        return self._ain_cluster_ingress_username
1
1    def set_ain_cluster_ingress_username(
1        self, ain_cluster_ingress_username: str
1    ) -> None:
1        self._ain_cluster_ingress_username = ain_cluster_ingress_username
1
1    def get_ain_cluster_ingress_password(self) -> str:
1        return self._ain_cluster_ingress_password
1
1    def set_ain_cluster_ingress_password(
1        self, ain_cluster_ingress_password: str
1    ) -> None:
1        self._ain_cluster_ingress_password = ain_cluster_ingress_password
1
1    def get_ain_cluster_ingress_time_zone(self) -> str:
1        return self._ain_cluster_ingress_time_zone
1
1    def set_ain_cluster_ingress_time_zone(
1        self, ain_cluster_ingress_time_zone: str
1    ) -> None:
1        self._ain_cluster_ingress_time_zone = ain_cluster_ingress_time_zone
1
1
1@singleton
1class AINodeDescriptor(object):
1    def __init__(self):
1        self._config = AINodeConfig()
1        self._load_config_from_file()
1        logger.info("AINodeDescriptor is init successfully.")
1
1    def _load_config_from_file(self) -> None:
1        system_properties_file = os.path.join(
1            self._config.get_ain_system_dir(), AINODE_SYSTEM_FILE_NAME
1        )
1        if os.path.exists(system_properties_file):
1            system_configs = load_properties(system_properties_file)
1            if "ainode_id" in system_configs:
1                self._config.set_ainode_id(int(system_configs["ainode_id"]))
1
1        git_file = os.path.join(
1            AINODE_ROOT_DIR, AINODE_ROOT_CONF_DIRECTORY_NAME, AINODE_CONF_GIT_FILE_NAME
1        )
1        if os.path.exists(git_file):
1            git_configs = load_properties(git_file)
1            if "git.commit.id.abbrev" in git_configs:
1                build_info = git_configs["git.commit.id.abbrev"]
1                if "git.dirty" in git_configs:
1                    if git_configs["git.dirty"] == "true":
1                        build_info += "-dev"
1                self._config.set_build_info(build_info)
1
1        pom_file = os.path.join(
1            AINODE_ROOT_DIR, AINODE_ROOT_CONF_DIRECTORY_NAME, AINODE_CONF_POM_FILE_NAME
1        )
1        if os.path.exists(pom_file):
1            pom_configs = load_properties(pom_file)
1            if "version" in pom_configs:
1                self._config.set_version_info(pom_configs["version"])
1
1        conf_file = os.path.join(AINODE_CONF_DIRECTORY_NAME, AINODE_CONF_FILE_NAME)
1        if not os.path.exists(conf_file):
1            logger.info(
1                "Cannot find AINode config file '{}', use default configuration.".format(
1                    conf_file
1                )
1            )
1            return
1
1        # noinspection PyBroadException
1        try:
1            file_configs = load_properties(conf_file)
1
1            config_keys = file_configs.keys()
1
1            if "ain_rpc_address" in config_keys:
1                self._config.set_ain_rpc_address(file_configs["ain_rpc_address"])
1
1            if "ain_rpc_port" in config_keys:
1                self._config.set_ain_rpc_port(int(file_configs["ain_rpc_port"]))
1
1            if "ain_inference_batch_interval_in_ms" in config_keys:
1                self._config.set_ain_inference_batch_interval_in_ms(
1                    int(file_configs["ain_inference_batch_interval_in_ms"])
1                )
1
1            if "ain_inference_model_mem_usage_map" in config_keys:
1                self._config.set_ain_inference_model_mem_usage_map(
1                    eval(file_configs["ain_inference_model_mem_usage_map"])
1                )
1
1            if "ain_inference_memory_usage_ratio" in config_keys:
1                self._config.set_ain_inference_memory_usage_ratio(
1                    float(file_configs["ain_inference_memory_usage_ratio"])
1                )
1
1            if "ain_inference_extra_memory_ratio" in config_keys:
1                self._config.set_ain_inference_extra_memory_ratio(
1                    float(file_configs["ain_inference_extra_memory_ratio"])
1                )
1
1            if "ain_models_dir" in config_keys:
1                self._config.set_ain_models_dir(file_configs["ain_models_dir"])
1
1            if "ain_system_dir" in config_keys:
1                self._config.set_ain_system_dir(file_configs["ain_system_dir"])
1
1            if "ain_seed_config_node" in config_keys:
1                self._config.set_ain_target_config_node_list(
1                    file_configs["ain_seed_config_node"]
1                )
1
1            if "cluster_name" in config_keys:
1                self._config.set_cluster_name(file_configs["cluster_name"])
1
1            if "ain_thrift_compression_enabled" in config_keys:
1                self._config.set_ain_thrift_compression_enabled(
1                    int(file_configs["ain_thrift_compression_enabled"])
1                )
1
1            if "ain_cluster_ingress_ssl_enabled" in config_keys:
1                self._config.set_ain_cluster_ingress_ssl_enabled(
1                    int(file_configs["ain_cluster_ingress_ssl_enabled"])
1                )
1
1            if "ain_thrift_ssl_cert_file" in config_keys:
1                self._config.set_ain_thrift_ssl_cert_file(
1                    file_configs["ain_thrift_ssl_cert_file"]
1                )
1
1            if "ain_thrift_ssl_key_file" in config_keys:
1                self._config.set_ain_thrift_ssl_key_file(
1                    file_configs["ain_thrift_ssl_key_file"]
1                )
1
1            if "ain_logs_dir" in config_keys:
1                log_dir = file_configs["ain_logs_dir"]
1                self._config.set_ain_logs_dir(log_dir)
1
1            if "ain_cluster_ingress_address" in config_keys:
1                self._config.set_ain_cluster_ingress_address(
1                    file_configs["ain_cluster_ingress_address"]
1                )
1
1            if "ain_cluster_ingress_port" in config_keys:
1                self._config.set_ain_cluster_ingress_port(
1                    int(file_configs["ain_cluster_ingress_port"])
1                )
1
1            if "ain_cluster_ingress_username" in config_keys:
1                self._config.set_ain_cluster_ingress_username(
1                    file_configs["ain_cluster_ingress_username"]
1                )
1
1            if "ain_cluster_ingress_password" in config_keys:
1                self._config.set_ain_cluster_ingress_password(
1                    file_configs["ain_cluster_ingress_password"]
1                )
1
1            if "ain_cluster_ingress_time_zone" in config_keys:
1                self._config.set_ain_cluster_ingress_time_zone(
1                    file_configs["ain_cluster_ingress_time_zone"]
1                )
1
1        except BadNodeUrlError:
1            logger.warning("Cannot load AINode conf file, use default configuration.")
1
1        except Exception as e:
1            logger.warning(
1                "Cannot load AINode conf file caused by: {}, use default configuration. ".format(
1                    e
1                )
1            )
1
1    def get_config(self) -> AINodeConfig:
1        return self._config
1
1
1def load_properties(filepath, sep="=", comment_char="#"):
1    """
1    Read the file passed as parameter as a properties file.
1    """
1    props = {}
1    with open(filepath, "rt") as f:
1        for line in f:
1            l = line.strip()
1            if l and not l.startswith(comment_char):
1                key_value = l.split(sep)
1                key = key_value[0].strip()
1                value = sep.join(key_value[1:]).strip().strip('"')
1                props[key] = value
1    return props
1
1
1def parse_endpoint_url(endpoint_url: str) -> TEndPoint:
1    """Parse TEndPoint from a given endpoint url.
1    Args:
1        endpoint_url: an endpoint url, format: ip:port
1    Returns:
1        TEndPoint
1    Raises:
1        BadNodeUrlError
1    """
1    split = endpoint_url.split(":")
1    if len(split) != 2:
1        raise BadNodeUrlError(endpoint_url)
1
1    ip = split[0]
1    try:
1        port = int(split[1])
1        result = TEndPoint(ip, port)
1        return result
1    except ValueError:
1        raise BadNodeUrlError(endpoint_url)
1