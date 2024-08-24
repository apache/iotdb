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
import sys
import threading
from datetime import datetime

import psutil
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from iotdb.ainode.client import client_manager
from iotdb.ainode.config import descriptor
from iotdb.ainode.constant import AINODE_SYSTEM_FILE_NAME
from iotdb.ainode.handler import AINodeRPCServiceHandler
from iotdb.ainode.log import logger
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.common.ttypes import TAINodeConfiguration, TAINodeLocation, TEndPoint, TNodeResource
from iotdb.thrift.confignode.ttypes import TNodeVersionInfo


class RPCService(threading.Thread):
    def __init__(self):
        self.exit_code = 0
        super().__init__()
        processor = IAINodeRPCService.Processor(handler=AINodeRPCServiceHandler())
        transport = TSocket.TServerSocket(host=descriptor.get_config().get_ain_inference_rpc_address(),
                                          port=descriptor.get_config().get_ain_inference_rpc_port())
        transport_factory = TTransport.TFramedTransportFactory()
        if descriptor.get_config().get_ain_thrift_compression_enabled():
            protocol_factory = TCompactProtocol.TCompactProtocolFactory()
        else:
            protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()

        self.__pool_server = TServer.TThreadPoolServer(processor, transport, transport_factory, protocol_factory)

    def run(self) -> None:
        logger.info("The RPC service thread begin to run...")
        try:
            self.__pool_server.serve()
        except Exception as e:
            self.exit_code = 1
            logger.error(e)


class AINode(object):
    def __init__(self):
        self.__rpc_service = RPCService()

    def start(self) -> None:
        logger.info('IoTDB-AINode is starting...')
        system_path = descriptor.get_config().get_ain_system_dir()
        system_properties_file = os.path.join(descriptor.get_config().get_ain_system_dir(), AINODE_SYSTEM_FILE_NAME)
        if not os.path.exists(system_path):
            try:
                os.makedirs(system_path)
                os.chmod(system_path, 0o777)
            except PermissionError as e:
                logger.error(e)
                raise e

        if not os.path.exists(system_properties_file):
            # If the system.properties file does not exist, the AINode will register to ConfigNode.
            try:
                logger.info('IoTDB-AINode is registering to ConfigNode...')
                ainode_id = client_manager.borrow_config_node_client().node_register(
                    descriptor.get_config().get_cluster_name(),
                    self._generate_configuration(),
                    self._generate_version_info())
                descriptor.get_config().set_ainode_id(ainode_id)
                system_properties = {
                    'ainode_id': ainode_id,
                    'cluster_name': descriptor.get_config().get_cluster_name(),
                    'iotdb_version': descriptor.get_config().get_version_info(),
                    'commit_id': descriptor.get_config().get_build_info(),
                    'ain_rpc_address': descriptor.get_config().get_ain_inference_rpc_address(),
                    'ain_rpc_port': descriptor.get_config().get_ain_inference_rpc_port(),
                    'config_node_list': descriptor.get_config().get_ain_target_config_node_list(),
                }
                with open(system_properties_file, 'w') as f:
                    f.write('#' + str(datetime.now()) + '\n')
                    for key, value in system_properties.items():
                        f.write(key + '=' + str(value) + '\n')

            except Exception as e:
                logger.error('IoTDB-AINode failed to register to ConfigNode: {}'.format(e))
                sys.exit(1)
        else:
            # If the system.properties file does exist, the AINode will just restart.
            try:
                logger.info('IoTDB-AINode is restarting...')
                client_manager.borrow_config_node_client().node_restart(
                    descriptor.get_config().get_cluster_name(),
                    self._generate_configuration(),
                    self._generate_version_info())

            except Exception as e:
                logger.error('IoTDB-AINode failed to restart: {}'.format(e))
                sys.exit(1)

        self.__rpc_service.start()
        self.__rpc_service.join(1)
        if self.__rpc_service.exit_code != 0:
            return

        logger.info('IoTDB-AINode has successfully started.')

    @staticmethod
    def _generate_configuration() -> TAINodeConfiguration:
        location = TAINodeLocation(descriptor.get_config().get_ainode_id(),
                                   TEndPoint(descriptor.get_config().get_ain_inference_rpc_address(),
                                             descriptor.get_config().get_ain_inference_rpc_port()))
        resource = TNodeResource(
            int(psutil.cpu_count()),
            int(psutil.virtual_memory()[0])
        )

        return TAINodeConfiguration(location, resource)

    @staticmethod
    def _generate_version_info() -> TNodeVersionInfo:
        return TNodeVersionInfo(descriptor.get_config().get_version_info(),
                                descriptor.get_config().get_build_info())
