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
import signal
import threading
from datetime import datetime

import psutil

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import AINODE_SYSTEM_FILE_NAME
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.rpc.client import ClientManager
from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
from iotdb.ainode.core.rpc.service import AINodeRPCService
from iotdb.thrift.common.ttypes import (
    TAINodeConfiguration,
    TAINodeLocation,
    TEndPoint,
    TNodeResource,
)
from iotdb.thrift.confignode.ttypes import TNodeVersionInfo

logger = Logger()


def _generate_configuration() -> TAINodeConfiguration:
    location = TAINodeLocation(
        AINodeDescriptor().get_config().get_ainode_id(),
        TEndPoint(
            AINodeDescriptor().get_config().get_ain_rpc_address(),
            AINodeDescriptor().get_config().get_ain_rpc_port(),
        ),
    )
    resource = TNodeResource(int(psutil.cpu_count()), int(psutil.virtual_memory()[0]))

    return TAINodeConfiguration(location, resource)


def _generate_version_info() -> TNodeVersionInfo:
    return TNodeVersionInfo(
        AINodeDescriptor().get_config().get_version_info(),
        AINodeDescriptor().get_config().get_build_info(),
    )


def _check_path_permission():
    system_path = AINodeDescriptor().get_config().get_ain_system_dir()
    if not os.path.exists(system_path):
        try:
            os.makedirs(system_path)
        except PermissionError as e:
            logger.error(e)
            raise e


def _generate_system_properties(ainode_id: int):
    return {
        "ainode_id": ainode_id,
        "cluster_name": AINodeDescriptor().get_config().get_cluster_name(),
        "iotdb_version": AINodeDescriptor().get_config().get_version_info(),
        "commit_id": AINodeDescriptor().get_config().get_build_info(),
        "ain_rpc_address": AINodeDescriptor().get_config().get_ain_rpc_address(),
        "ain_rpc_port": AINodeDescriptor().get_config().get_ain_rpc_port(),
        "config_node_list": AINodeDescriptor()
        .get_config()
        .get_ain_target_config_node_list(),
    }


class AINode:
    def __init__(self):
        self._rpc_service = None
        self._rpc_handler = None
        self._stop_event = None

    def start(self):
        _check_path_permission()
        system_properties_file = os.path.join(
            AINodeDescriptor().get_config().get_ain_system_dir(),
            AINODE_SYSTEM_FILE_NAME,
        )
        if not os.path.exists(system_properties_file):
            # If the system.properties file does not exist, the AINode will register to IoTDB cluster.
            try:
                logger.info("IoTDB-AINode is registering to IoTDB cluster...")
                ainode_id = (
                    ClientManager()
                    .borrow_config_node_client()
                    .node_register(
                        AINodeDescriptor().get_config().get_cluster_name(),
                        _generate_configuration(),
                        _generate_version_info(),
                    )
                )
                AINodeDescriptor().get_config().set_ainode_id(ainode_id)
                system_properties = _generate_system_properties(ainode_id)
                with open(system_properties_file, "w") as f:
                    f.write("#" + str(datetime.now()) + "\n")
                    for key, value in system_properties.items():
                        f.write(key + "=" + str(value) + "\n")
            except Exception as e:
                logger.error(
                    "IoTDB-AINode failed to register to IoTDB cluster: {}".format(e)
                )
                raise e
        else:
            # If the system.properties file does exist, the AINode will just restart.
            try:
                logger.info("IoTDB-AINode is restarting...")
                ClientManager().borrow_config_node_client().node_restart(
                    AINodeDescriptor().get_config().get_cluster_name(),
                    _generate_configuration(),
                    _generate_version_info(),
                )
            except Exception as e:
                logger.error("IoTDB-AINode failed to restart: {}".format(e))
                raise e

        # Start the RPC service
        self._rpc_handler = AINodeRPCServiceHandler(ainode=self)
        self._rpc_service = AINodeRPCService(self._rpc_handler)
        self._rpc_service.start()
        self._rpc_service.join(1)
        if self._rpc_service.exit_code != 0:
            logger.info("IoTDB-AINode failed to start, please check previous logs.")
            return

        logger.info("IoTDB-AINode has successfully started.")

        # Register stop hook
        self._stop_event = threading.Event()
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._rpc_service.join()

    def _handle_signal(self, signum, frame):
        signal_name = {signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}.get(
            signum, f"SIGNAL {signum}"
        )

        logger.info(f"IoTDB-AINode receives {signal_name}, initiating graceful stop...")
        self.stop()

    def stop(self):
        if not self._stop_event.is_set():
            self._stop_event.set()
            self._rpc_handler.stop()
            self._rpc_service.stop()
