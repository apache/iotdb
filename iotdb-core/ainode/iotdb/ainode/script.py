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
            # Delete the current node
            if len(arguments) == 2:
                target_ainode_id = descriptor.get_config().get_ainode_id()
                target_rpc_address = descriptor.get_config().get_ain_inference_rpc_address()
                target_rpc_port = descriptor.get_config().get_ain_inference_rpc_port()

            # Delete the node with a given id
            elif len(arguments) == 3:
                target_ainode_id = arguments[2]

                # ainode id
                ainode_configuration_map = client_manager.borrow_config_node_client().get_ainode_configuration(target_ainode_id)

                end_point = ainode_configuration_map[target_ainode_id].location.internalEndPoint
                target_rpc_address = end_point.ip
                target_rpc_port = end_point.port

                if not end_point:
                    raise MissingConfigError("NodeId: {} not found in cluster ".format(target_ainode_id))

                logger.info('Got target AINode id: {}'.format(target_ainode_id))

            else:
                raise MissingConfigError("Invalid command")

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

if __name__ == '__main__':
    main()