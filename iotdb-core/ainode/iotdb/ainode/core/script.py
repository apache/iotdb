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
1import shutil
1import sys
1
1import torch.multiprocessing as mp
1
1from iotdb.ainode.core.ai_node import AINode
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import TSStatusCode
1from iotdb.ainode.core.exception import MissingConfigError
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.rpc.client import ClientManager
1from iotdb.thrift.common.ttypes import TAINodeLocation, TEndPoint
1
1logger = Logger()
1
1
1def remove_ainode(arguments):
1    # Delete the current node
1    if len(arguments) == 2:
1        target_ainode_id = AINodeDescriptor().get_config().get_ainode_id()
1        target_rpc_address = AINodeDescriptor().get_config().get_ain_rpc_address()
1        target_rpc_port = AINodeDescriptor().get_config().get_ain_rpc_port()
1
1    # Delete the node with a given id
1    elif len(arguments) == 3:
1        target_ainode_id = int(arguments[2])
1        ainode_configuration_map = (
1            ClientManager()
1            .borrow_config_node_client()
1            .get_ainode_configuration(target_ainode_id)
1        )
1
1        end_point = ainode_configuration_map[target_ainode_id].location.internalEndPoint
1        target_rpc_address = end_point.ip
1        target_rpc_port = end_point.port
1
1        if not end_point:
1            raise MissingConfigError(
1                "NodeId: {} not found in cluster ".format(target_ainode_id)
1            )
1
1        logger.info("Got target AINode id: {}".format(target_ainode_id))
1
1    else:
1        raise MissingConfigError("Invalid command")
1
1    location = TAINodeLocation(
1        target_ainode_id, TEndPoint(target_rpc_address, target_rpc_port)
1    )
1    status = ClientManager().borrow_config_node_client().node_remove(location)
1
1    if status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
1        logger.info("IoTDB-AINode has successfully removed.")
1        if os.path.exists(AINodeDescriptor().get_config().get_ain_models_dir()):
1            shutil.rmtree(AINodeDescriptor().get_config().get_ain_models_dir())
1
1
1def main():
1    arguments = sys.argv
1    # load config
1    AINodeDescriptor()
1    if len(arguments) == 1:
1        logger.info("Command line argument must be specified.")
1        return
1    command = arguments[1]
1    if command == "start":
1        try:
1            mp.set_start_method("spawn", force=True)
1            logger.info(f"Current multiprocess start method: {mp.get_start_method()}")
1            logger.info("IoTDB-AINode is starting...")
1            ai_node = AINode()
1            ai_node.start()
1        except Exception as e:
1            logger.error("Start AINode failed, because of: {}".format(e))
1            sys.exit(1)
1    # TODO: remove the following function, and add a destroy script
1    elif command == "remove":
1        try:
1            logger.info("Removing AINode...")
1            remove_ainode(arguments)
1        except Exception as e:
1            logger.error("Remove AINode failed, because of: {}".format(e))
1            sys.exit(1)
1    else:
1        logger.warning("Unknown argument: {}.".format(command))
1
1
1if __name__ == "__main__":
1    main()
1