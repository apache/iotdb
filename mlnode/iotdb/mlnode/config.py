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

from dynaconf import Dynaconf

from iotdb.mlnode.constant import (MLNODE_CONF_DIRECTORY_NAME,
                                   MLNODE_CONF_FILE_NAME)
from iotdb.mlnode.exception import BadNodeUrlError
from iotdb.mlnode.log import logger
from iotdb.mlnode.util import parse_endpoint_url
from iotdb.thrift.common.ttypes import TEndPoint


class MLNodeConfig(object):
    def __init__(self):
        # Used for connection of DataNode/ConfigNode clients
        self.__mn_rpc_address: str = "127.0.0.1"
        self.__mn_rpc_port: int = 10810

        # Target ConfigNode to be connected by MLNode
        self.__mn_target_config_node: TEndPoint = TEndPoint("127.0.0.1", 10710)

        # Target DataNode to be connected by MLNode
        self.__mn_target_data_node: TEndPoint = TEndPoint("127.0.0.1", 10730)

    def get_mn_rpc_address(self) -> str:
        return self.__mn_rpc_address

    def set_mn_rpc_address(self, mn_rpc_address: str) -> None:
        self.__mn_rpc_address = mn_rpc_address

    def get_mn_rpc_port(self) -> int:
        return self.__mn_rpc_port

    def set_mn_rpc_port(self, mn_rpc_port: int) -> None:
        self.__mn_rpc_port = mn_rpc_port

    def get_mn_target_config_node(self) -> TEndPoint:
        return self.__mn_target_config_node

    def set_mn_target_config_node(self, mn_target_config_node: str) -> None:
        self.__mn_target_config_node = parse_endpoint_url(mn_target_config_node)

    def get_mn_target_data_node(self) -> TEndPoint:
        return self.__mn_target_data_node

    def set_mn_target_data_node(self, mn_target_data_node: str) -> None:
        self.__mn_target_data_node = parse_endpoint_url(mn_target_data_node)


class MLNodeDescriptor(object):
    def __init__(self):
        self.__config = MLNodeConfig()
        self.__load_config_from_file()

    def __load_config_from_file(self) -> None:
        conf_file = os.path.join(os.getcwd(), MLNODE_CONF_DIRECTORY_NAME, MLNODE_CONF_FILE_NAME)
        if not os.path.exists(conf_file):
            logger.info("Cannot find MLNode config file '{}', use default configuration.".format(conf_file))
            return

        logger.info("Start to read MLNode config file '{}'...".format(conf_file))

        # noinspection PyBroadException
        try:
            file_configs = Dynaconf(
                envvar_prefix="DYNACONF",
                settings_files=[conf_file],
            )

            if file_configs.mn_rpc_address is not None:
                self.__config.set_mn_rpc_address(file_configs.mn_rpc_address)

            if file_configs.mn_rpc_port is not None:
                self.__config.set_mn_rpc_port(file_configs.mn_rpc_port)

            if file_configs.mn_target_config_node is not None:
                self.__config.set_mn_target_config_node(file_configs.mn_target_config_node)

            if file_configs.mn_target_data_node is not None:
                self.__config.set_mn_target_data_node(file_configs.mn_target_data_node)
        except BadNodeUrlError:
            logger.warn("Cannot load MLNode conf file, use default configuration.")
        except Exception as e:
            logger.warn("Cannot load MLNode conf file, use default configuration. {}".format(e))

    def get_config(self) -> MLNodeConfig:
        return self.__config


config = MLNodeDescriptor().get_config()
