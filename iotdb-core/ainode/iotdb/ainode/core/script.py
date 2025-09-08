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

import torch.multiprocessing as mp

from iotdb.ainode.core.ai_node import AINode
from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.exception import MissingConfigError
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.rpc.client import ClientManager
from iotdb.thrift.common.ttypes import (
    TAINodeLocation,
    TEndPoint,
)

logger = Logger()


def remove_ainode(arguments):
    # Delete the current node
    if len(arguments) == 2:
        target_ainode_id = AINodeDescriptor().get_config().get_ainode_id()
        target_rpc_address = AINodeDescriptor().get_config().get_ain_rpc_address()
        target_rpc_port = AINodeDescriptor().get_config().get_ain_rpc_port()

    # Delete the node with a given id
    elif len(arguments) == 3:
        target_ainode_id = int(arguments[2])
        ainode_configuration_map = (
            ClientManager()
            .borrow_config_node_client()
            .get_ainode_configuration(target_ainode_id)
        )

        end_point = ainode_configuration_map[target_ainode_id].location.internalEndPoint
        target_rpc_address = end_point.ip
        target_rpc_port = end_point.port

        if not end_point:
            raise MissingConfigError(
                "NodeId: {} not found in cluster ".format(target_ainode_id)
            )

        logger.info("Got target AINode id: {}".format(target_ainode_id))

    else:
        raise MissingConfigError("Invalid command")

    location = TAINodeLocation(
        target_ainode_id, TEndPoint(target_rpc_address, target_rpc_port)
    )
    status = ClientManager().borrow_config_node_client().node_remove(location)

    if status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
        logger.info("IoTDB-AINode has successfully removed.")
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
    if command == "start":
        try:
            mp.set_start_method("spawn", force=True)
            logger.info(f"Current multiprocess start method: {mp.get_start_method()}")
            logger.info("IoTDB-AINode is starting...")
            ai_node = AINode()
            ai_node.start()
        except Exception as e:
            logger.error("Start AINode failed, because of: {}".format(e))
            sys.exit(1)
    # TODO: remove the following function, and add a destroy script
    elif command == "remove":
        try:
            logger.info("Removing AINode...")
            remove_ainode(arguments)
        except Exception as e:
            logger.error("Remove AINode failed, because of: {}".format(e))
            sys.exit(1)
    else:
        logger.warning("Unknown argument: {}.".format(command))


if __name__ == "__main__":
    main()
