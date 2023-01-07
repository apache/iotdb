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

from dynaconf import Dynaconf

from iotdb.thrift.common.ttypes import TEndPoint

from iotdb.mlnode.log import logger


class MlNodeConfig(object):
    def __init__(self):
        self.__mn_rpc_address: str = "127.0.0.1"
        self.__mn_rpc_port: int = 10810
        self.__mn_target_config_node_list: list = [TEndPoint("127.0.0.1", 10710)]
        self.__mn_target_data_node_list: list = [TEndPoint("127.0.0.1", 10730)]

    def get_mn_rpc_address(self) -> str:
        return self.__mn_rpc_address

    def set_mn_rpc_address(self, mn_rpc_address: str) -> None:
        self.__mn_rpc_address = mn_rpc_address

    def get_mn_rpc_port(self) -> int:
        return self.__mn_rpc_port

    def set_mn_rpc_port(self, mn_rpc_port: int) -> None:
        self.__mn_rpc_port = mn_rpc_port


class MlNodeDescriptor(object):
    def __init__(self):
        self.__config = MlNodeConfig()
        self.__load_config_from_file()

    def __load_config_from_file(self) -> None:
        url = '../../assembly/settings.toml'
        logger.info("Start to read config file '{}'".format(url))
        file_configs = Dynaconf(
            envvar_prefix="DYNACONF",
            settings_files=[url],
        )

        if file_configs.mn_rpc_address is not None:
            self.__config.set_mn_rpc_address(file_configs.mn_rpc_address)

        if file_configs.mn_rpc_port is not None:
            self.__config.set_mn_rpc_port(file_configs.mn_rpc_port)

    def get_config(self) -> MlNodeConfig:
        return self.__config


config = MlNodeDescriptor().get_config()
