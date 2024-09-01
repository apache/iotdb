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
import os

from iotdb.ainode.constant import (AINODE_CONF_DIRECTORY_NAME,
                                   AINODE_CONF_FILE_NAME,
                                   AINODE_MODELS_DIR, AINODE_LOG_DIR, AINODE_SYSTEM_DIR, AINODE_INFERENCE_RPC_ADDRESS,
                                   AINODE_INFERENCE_RPC_PORT, AINODE_THRIFT_COMPRESSION_ENABLED,
                                   AINODE_SYSTEM_FILE_NAME, AINODE_CLUSTER_NAME, AINODE_VERSION_INFO, AINODE_BUILD_INFO,
                                   AINODE_CONF_GIT_FILE_NAME, AINODE_CONF_POM_FILE_NAME, AINODE_ROOT_DIR,
                                   AINODE_ROOT_CONF_DIRECTORY_NAME)
from iotdb.ainode.exception import BadNodeUrlError
from iotdb.ainode.log import logger, set_logger
from iotdb.ainode.util import parse_endpoint_url
from iotdb.thrift.common.ttypes import TEndPoint


class AINodeConfig(object):
    def __init__(self):
        # Used for connection of DataNode/ConfigNode clients
        self.__ain_inference_rpc_address: str = AINODE_INFERENCE_RPC_ADDRESS
        self.__ain_inference_rpc_port: int = AINODE_INFERENCE_RPC_PORT

        # log directory
        self.__ain_logs_dir: str = AINODE_LOG_DIR

        # Directory to save models
        self.__ain_models_dir = AINODE_MODELS_DIR

        self.__ain_system_dir = AINODE_SYSTEM_DIR

        # Whether to enable compression for thrift
        self.__ain_thrift_compression_enabled = AINODE_THRIFT_COMPRESSION_ENABLED

        # Cache number of model storage to avoid repeated loading
        self.__mn_model_storage_cache_size = 30

        # Maximum number of training model tasks, otherwise the task is pending
        self.__mn_task_pool_size = 1

        # Maximum number of trials to be explored in a tuning task
        self.__mn_tuning_trial_num = 20

        # Concurrency of trials in a tuning task
        self.__mn_tuning_trial_concurrency = 4

        # Target ConfigNode to be connected by AINode
        self.__ain_target_config_node_list: TEndPoint = TEndPoint("127.0.0.1", 10710)

        # Target DataNode to be connected by AINode
        self.__mn_target_data_node: TEndPoint = TEndPoint("127.0.0.1", 10780)

        # use for node management
        self.__ainode_id = 0
        self.__cluster_name = AINODE_CLUSTER_NAME

        self.__version_info = AINODE_VERSION_INFO
        self.__build_info = AINODE_BUILD_INFO

    def get_cluster_name(self) -> str:
        return self.__cluster_name

    def set_cluster_name(self, cluster_name: str) -> None:
        self.__cluster_name = cluster_name

    def get_version_info(self) -> str:
        return self.__version_info

    def get_ainode_id(self) -> int:
        return self.__ainode_id

    def set_ainode_id(self, id: int) -> None:
        self.__ainode_id = id

    def get_build_info(self) -> str:
        return self.__build_info

    def set_build_info(self, build_info: str) -> None:
        self.__build_info = build_info

    def set_version_info(self, version_info: str) -> None:
        self.__version_info = version_info

    def get_ain_inference_rpc_address(self) -> str:
        return self.__ain_inference_rpc_address

    def set_ain_inference_rpc_address(self, ain_inference_rpc_address: str) -> None:
        self.__ain_inference_rpc_address = ain_inference_rpc_address

    def get_ain_inference_rpc_port(self) -> int:
        return self.__ain_inference_rpc_port

    def set_ain_inference_rpc_port(self, ain_inference_rpc_port: int) -> None:
        self.__ain_inference_rpc_port = ain_inference_rpc_port

    def get_ain_logs_dir(self) -> str:
        return self.__ain_logs_dir

    def set_ain_logs_dir(self, ain_logs_dir: str) -> None:
        self.__ain_logs_dir = ain_logs_dir

    def get_ain_models_dir(self) -> str:
        return self.__ain_models_dir

    def set_ain_models_dir(self, ain_models_dir: str) -> None:
        self.__ain_models_dir = ain_models_dir

    def get_ain_system_dir(self) -> str:
        return self.__ain_system_dir

    def set_ain_system_dir(self, ain_system_dir: str) -> None:
        self.__ain_system_dir = ain_system_dir

    def get_ain_thrift_compression_enabled(self) -> bool:
        return self.__ain_thrift_compression_enabled

    def set_ain_thrift_compression_enabled(self, ain_thrift_compression_enabled: int) -> None:
        self.__ain_thrift_compression_enabled = ain_thrift_compression_enabled

    def get_mn_model_storage_cache_size(self) -> int:
        return self.__mn_model_storage_cache_size

    def set_mn_model_storage_cache_size(self, mn_model_storage_cache_size: int) -> None:
        self.__mn_model_storage_cache_size = mn_model_storage_cache_size

    def get_mn_mn_task_pool_size(self) -> int:
        return self.__mn_task_pool_size

    def set_mn_task_pool_size(self, mn_task_pool_size: int) -> None:
        self.__mn_task_pool_size = mn_task_pool_size

    def get_mn_tuning_trial_num(self) -> int:
        return self.__mn_tuning_trial_num

    def set_mn_tuning_trial_num(self, mn_tuning_trial_num: int) -> None:
        self.__mn_tuning_trial_num = mn_tuning_trial_num

    def get_mn_tuning_trial_concurrency(self) -> int:
        return self.__mn_tuning_trial_concurrency

    def set_mn_tuning_trial_concurrency(self, mn_tuning_trial_concurrency: int) -> None:
        self.__mn_tuning_trial_concurrency = mn_tuning_trial_concurrency

    def get_ain_target_config_node_list(self) -> TEndPoint:
        return self.__ain_target_config_node_list

    def set_ain_target_config_node_list(self, ain_target_config_node_list: str) -> None:
        self.__ain_target_config_node_list = parse_endpoint_url(ain_target_config_node_list)

    def get_mn_target_data_node(self) -> TEndPoint:
        return self.__mn_target_data_node

    def set_mn_target_data_node(self, mn_target_data_node: str) -> None:
        self.__mn_target_data_node = parse_endpoint_url(mn_target_data_node)


class AINodeDescriptor(object):
    _instance = None
    _first_init = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._first_init:
            self.__config = AINodeConfig()
            self.load_config_from_file()
            self._first_init = True

    def load_properties(self, filepath, sep='=', comment_char='#'):
        """
        Read the file passed as parameter as a properties file.
        """
        props = {}
        with open(filepath, "rt") as f:
            for line in f:
                l = line.strip()
                if l and not l.startswith(comment_char):
                    key_value = l.split(sep)
                    key = key_value[0].strip()
                    value = sep.join(key_value[1:]).strip().strip('"')
                    props[key] = value
        return props

    def load_config_from_file(self) -> None:
        system_properties_file = os.path.join(self.__config.get_ain_system_dir(), AINODE_SYSTEM_FILE_NAME)
        if os.path.exists(system_properties_file):
            system_configs = self.load_properties(system_properties_file)
            if 'ainode_id' in system_configs:
                self.__config.set_ainode_id(int(system_configs['ainode_id']))

        git_file = os.path.join(AINODE_ROOT_DIR, AINODE_ROOT_CONF_DIRECTORY_NAME, AINODE_CONF_GIT_FILE_NAME)
        if os.path.exists(git_file):
            git_configs = self.load_properties(git_file)
            if 'git.commit.id.abbrev' in git_configs:
                build_info = git_configs['git.commit.id.abbrev']
                if 'git.dirty' in git_configs:
                    if git_configs['git.dirty'] == "true":
                        build_info += "-dev"
                self.__config.set_build_info(build_info)

        pom_file = os.path.join(AINODE_ROOT_DIR, AINODE_ROOT_CONF_DIRECTORY_NAME, AINODE_CONF_POM_FILE_NAME)
        if os.path.exists(pom_file):
            pom_configs = self.load_properties(pom_file)
            if 'version' in pom_configs:
                self.__config.set_version_info(pom_configs['version'])

        conf_file = os.path.join(AINODE_CONF_DIRECTORY_NAME, AINODE_CONF_FILE_NAME)
        if not os.path.exists(conf_file):
            logger.info("Cannot find AINode config file '{}', use default configuration.".format(conf_file))
            return

        logger.info("Start to read AINode config file '{}'".format(conf_file))

        # noinspection PyBroadException
        try:
            file_configs = self.load_properties(conf_file)

            config_keys = file_configs.keys()

            if 'ain_inference_rpc_address' in config_keys:
                self.__config.set_ain_inference_rpc_address(file_configs['ain_inference_rpc_address'])

            if 'ain_inference_rpc_port' in config_keys:
                self.__config.set_ain_inference_rpc_port(int(file_configs['ain_inference_rpc_port']))

            if 'ain_logs_dir' in config_keys:
                self.__config.set_ain_logs_dir(file_configs['ain_logs_dir'])

            set_logger(self.__config.get_ain_logs_dir())

            if 'ain_models_dir' in config_keys:
                self.__config.set_ain_models_dir(file_configs['ain_models_dir'])

            if 'ain_system_dir' in config_keys:
                self.__config.set_ain_system_dir(file_configs['ain_system_dir'])

            if 'ain_seed_config_node' in config_keys:
                self.__config.set_ain_target_config_node_list(file_configs['ain_seed_config_node'])

            if 'cluster_name' in config_keys:
                self.__config.set_cluster_name(file_configs['cluster_name'])

            # AINODE_THRIFT_COMPRESSION_ENABLED
            if 'ain_thrift_compression_enabled' in config_keys:
                self.__config.set_ain_thrift_compression_enabled(int(file_configs['ain_thrift_compression_enabled']))

        except BadNodeUrlError:
            logger.warning("Cannot load AINode conf file, use default configuration.")

        except Exception as e:
            logger.warning("Cannot load AINode conf file caused by: {}, use default configuration. ".format(e))

    def get_config(self) -> AINodeConfig:
        return self.__config


# initialize a singleton
descriptor = AINodeDescriptor()
