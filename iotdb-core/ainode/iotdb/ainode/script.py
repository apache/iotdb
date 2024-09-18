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
from datetime import datetime

import psutil

from iotdb.ainode.client import ClientManager
from iotdb.ainode.config import AINodeDescriptor
from iotdb.ainode.constant import TSStatusCode, AINODE_SYSTEM_FILE_NAME
from iotdb.ainode.exception import MissingConfigError
from iotdb.ainode.log import Logger
from iotdb.ainode.service import RPCService
from iotdb.thrift.common.ttypes import TAINodeLocation, TEndPoint, TAINodeConfiguration, TNodeResource
from iotdb.thrift.confignode.ttypes import TNodeVersionInfo

logger = Logger()


def _generate_configuration() -> TAINodeConfiguration:
    location = TAINodeLocation(AINodeDescriptor().get_config().get_ainode_id(),
                               TEndPoint(AINodeDescriptor().get_config().get_ain_inference_rpc_address(),
                                         AINodeDescriptor().get_config().get_ain_inference_rpc_port()))
    resource = TNodeResource(
        int(psutil.cpu_count()),
        int(psutil.virtual_memory()[0])
    )

    return TAINodeConfiguration(location, resource)


def _generate_version_info() -> TNodeVersionInfo:
    return TNodeVersionInfo(AINodeDescriptor().get_config().get_version_info(),
                            AINodeDescriptor().get_config().get_build_info())


def _check_path_permission():
    system_path = AINodeDescriptor().get_config().get_ain_system_dir()
    if not os.path.exists(system_path):
        try:
            os.makedirs(system_path)
            os.chmod(system_path, 0o777)
        except PermissionError as e:
            logger.error(e)
            raise e


def start_ainode():
    _check_path_permission()
    system_properties_file = os.path.join(AINodeDescriptor().get_config().get_ain_system_dir(), AINODE_SYSTEM_FILE_NAME)
    if not os.path.exists(system_properties_file):
        # If the system.properties file does not exist, the AINode will register to ConfigNode.
        try:
            logger.info('IoTDB-AINode is registering to ConfigNode...')
            ainode_id = ClientManager().borrow_config_node_client().node_register(
                AINodeDescriptor().get_config().get_cluster_name(),
                _generate_configuration(),
                _generate_version_info())
            AINodeDescriptor().get_config().set_ainode_id(ainode_id)
            system_properties = {
                'ainode_id': ainode_id,
                'cluster_name': AINodeDescriptor().get_config().get_cluster_name(),
                'iotdb_version': AINodeDescriptor().get_config().get_version_info(),
                'commit_id': AINodeDescriptor().get_config().get_build_info(),
                'ain_rpc_address': AINodeDescriptor().get_config().get_ain_inference_rpc_address(),
                'ain_rpc_port': AINodeDescriptor().get_config().get_ain_inference_rpc_port(),
                'config_node_list': AINodeDescriptor().get_config().get_ain_target_config_node_list(),
            }
            with open(system_properties_file, 'w') as f:
                f.write('#' + str(datetime.now()) + '\n')
                for key, value in system_properties.items():
                    f.write(key + '=' + str(value) + '\n')

        except Exception as e:
            logger.error('IoTDB-AINode failed to register to ConfigNode: {}'.format(e))
            raise e
    else:
        # If the system.properties file does exist, the AINode will just restart.
        try:
            logger.info('IoTDB-AINode is restarting...')
            ClientManager().borrow_config_node_client().node_restart(
                AINodeDescriptor().get_config().get_cluster_name(),
                _generate_configuration(),
                _generate_version_info())

        except Exception as e:
            logger.error('IoTDB-AINode failed to restart: {}'.format(e))
            raise e

    rpc_service = RPCService()
    rpc_service.start()
    rpc_service.join(1)
    if rpc_service.exit_code != 0:
        return

    logger.info('IoTDB-AINode has successfully started.')


def remove_ainode(arguments):
    # Delete the current node
    if len(arguments) == 2:
        target_ainode_id = AINodeDescriptor().get_config().get_ainode_id()
        target_rpc_address = AINodeDescriptor().get_config().get_ain_inference_rpc_address()
        target_rpc_port = AINodeDescriptor().get_config().get_ain_inference_rpc_port()

    # Delete the node with a given id
    elif len(arguments) == 3:
        target_ainode_id = int(arguments[2])
        ainode_configuration_map = ClientManager().borrow_config_node_client().get_ainode_configuration(
            target_ainode_id)

        end_point = ainode_configuration_map[target_ainode_id].location.internalEndPoint
        target_rpc_address = end_point.ip
        target_rpc_port = end_point.port

        if not end_point:
            raise MissingConfigError("NodeId: {} not found in cluster ".format(target_ainode_id))

        logger.info('Got target AINode id: {}'.format(target_ainode_id))

    else:
        raise MissingConfigError("Invalid command")

    location = TAINodeLocation(target_ainode_id, TEndPoint(target_rpc_address, target_rpc_port))
    status = ClientManager().borrow_config_node_client().node_remove(location)

    if status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
        logger.info('IoTDB-AINode has successfully removed.')
        if os.path.exists(AINodeDescriptor().get_config().get_ain_models_dir()):
            shutil.rmtree(AINodeDescriptor().get_config().get_ain_models_dir())


def main():
    arguments = sys.argv
    # load config
    AINodeDescriptor()
    if len(arguments) == 1:
        logger.info("Command line argument must be specified.")
        return
    command = arguments[1]
    if command == 'start':
        try:
            logger.info('IoTDB-AINode is starting...')
            start_ainode()
        except Exception as e:
            logger.error("Start AINode failed, because of: {}".format(e))
            sys.exit(1)
    elif command == 'remove':
        try:
            logger.info("Removing AINode...")
            remove_ainode(arguments)
        except Exception as e:
            logger.error("Remove AINode failed, because of: {}".format(e))
            sys.exit(1)
    else:
        logger.warning("Unknown argument: {}.".format(command))


if __name__ == '__main__':
    main()
