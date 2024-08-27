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
import shutil
import sys

from iotdb.ainode.client import client_manager
from iotdb.ainode.config import descriptor
from iotdb.ainode.constant import TSStatusCode
from iotdb.ainode.exception import MissingConfigError
from iotdb.ainode.log import logger
from iotdb.ainode.service import AINode
from iotdb.thrift.common.ttypes import TAINodeLocation, TEndPoint

server: AINode = None
POINT_COLON = ":"

def main():
    global server
    arguments = sys.argv
    if len(arguments) == 1:
        logger.info("Command line argument must be specified.")
        return
    command = arguments[1]
    if command == 'start':
        try:
            server = AINode()
            server.start()
        except Exception as e:
            logger.error("Start AINode failed, because of: {}".format(e))
            sys.exit(1)
    elif command == 'remove':
        try:
            logger.info("Removing AINode...")
            if len(arguments) >= 3:
                target_ainode = arguments[2]
                # parameter pattern: <ainode-id> or <ip>:<rpc-port>
                ainode_info = target_ainode.split(POINT_COLON)
                target_ainode_id = -1

                # ainode id
                if len(ainode_info) == 1:
                    target_ainode_id = int(ainode_info[0])

                    ainode_configuration_map = client_manager.borrow_config_node_client().get_ainode_configuration(
                        target_ainode_id)

                    end_point = ainode_configuration_map[target_ainode_id].location.internalEndPoint
                    target_rpc_address = end_point.ip
                    target_rpc_port = end_point.port
                elif len(ainode_info) == 2:
                    target_rpc_address = ainode_info[0]
                    target_rpc_port = int(ainode_info[1])

                    ainode_configuration_map = client_manager.borrow_config_node_client().get_ainode_configuration(-1)

                    for cur_ainode_id, cur_configuration in ainode_configuration_map.items():
                        cur_end_point = cur_configuration.location.internalEndPoint
                        if cur_end_point.ip == target_rpc_address and cur_end_point.port == target_rpc_port:
                            target_ainode_id = cur_ainode_id
                            break
                    if target_ainode_id == -1:
                        raise MissingConfigError(
                            "Can't find ainode through {}:{}".format(target_rpc_port, target_rpc_address))
                else:
                    raise MissingConfigError("NodeId or IP:Port should be provided to remove AINode")

                logger.info('Got target AINode id: {}, address: {}, port: {}'
                            .format(target_ainode_id, target_rpc_address, target_rpc_port))
            else:
                target_ainode_id = descriptor.get_config().get_ainode_id()
                target_rpc_address = descriptor.get_config().get_ain_inference_rpc_address()
                target_rpc_port = descriptor.get_config().get_ain_inference_rpc_port()

            location = TAINodeLocation(target_ainode_id, TEndPoint(target_rpc_address, target_rpc_port))
            status = client_manager.borrow_config_node_client().node_remove(location)

            if status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info('IoTDB-AINode has successfully removed.')
                if os.path.exists(descriptor.get_config().get_ain_models_dir()):
                    shutil.rmtree(descriptor.get_config().get_ain_models_dir())

        except Exception as e:
            logger.error("Remove AINode failed, because of: {}".format(e))
            sys.exit(1)
    else:
        logger.warning("Unknown argument: {}.".format(command))
