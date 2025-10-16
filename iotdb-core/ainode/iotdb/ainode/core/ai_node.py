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
1import signal
1import threading
1from datetime import datetime
1
1import psutil
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import AINODE_SYSTEM_FILE_NAME
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.rpc.client import ClientManager
1from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
1from iotdb.ainode.core.rpc.service import AINodeRPCService
1from iotdb.thrift.common.ttypes import (
1    TAINodeConfiguration,
1    TAINodeLocation,
1    TEndPoint,
1    TNodeResource,
1)
1from iotdb.thrift.confignode.ttypes import TNodeVersionInfo
1
1logger = Logger()
1
1
1def _generate_configuration() -> TAINodeConfiguration:
1    location = TAINodeLocation(
1        AINodeDescriptor().get_config().get_ainode_id(),
1        TEndPoint(
1            AINodeDescriptor().get_config().get_ain_rpc_address(),
1            AINodeDescriptor().get_config().get_ain_rpc_port(),
1        ),
1    )
1    resource = TNodeResource(int(psutil.cpu_count()), int(psutil.virtual_memory()[0]))
1
1    return TAINodeConfiguration(location, resource)
1
1
1def _generate_version_info() -> TNodeVersionInfo:
1    return TNodeVersionInfo(
1        AINodeDescriptor().get_config().get_version_info(),
1        AINodeDescriptor().get_config().get_build_info(),
1    )
1
1
1def _check_path_permission():
1    system_path = AINodeDescriptor().get_config().get_ain_system_dir()
1    if not os.path.exists(system_path):
1        try:
1            os.makedirs(system_path)
1        except PermissionError as e:
1            logger.error(e)
1            raise e
1
1
1def _generate_system_properties(ainode_id: int):
1    return {
1        "ainode_id": ainode_id,
1        "cluster_name": AINodeDescriptor().get_config().get_cluster_name(),
1        "iotdb_version": AINodeDescriptor().get_config().get_version_info(),
1        "commit_id": AINodeDescriptor().get_config().get_build_info(),
1        "ain_rpc_address": AINodeDescriptor().get_config().get_ain_rpc_address(),
1        "ain_rpc_port": AINodeDescriptor().get_config().get_ain_rpc_port(),
1        "config_node_list": AINodeDescriptor()
1        .get_config()
1        .get_ain_target_config_node_list(),
1    }
1
1
1class AINode:
1    def __init__(self):
1        self._rpc_service = None
1        self._rpc_handler = None
1        self._stop_event = None
1
1    def start(self):
1        _check_path_permission()
1        system_properties_file = os.path.join(
1            AINodeDescriptor().get_config().get_ain_system_dir(),
1            AINODE_SYSTEM_FILE_NAME,
1        )
1        if not os.path.exists(system_properties_file):
1            # If the system.properties file does not exist, the AINode will register to IoTDB cluster.
1            try:
1                logger.info("IoTDB-AINode is registering to IoTDB cluster...")
1                ainode_id = (
1                    ClientManager()
1                    .borrow_config_node_client()
1                    .node_register(
1                        AINodeDescriptor().get_config().get_cluster_name(),
1                        _generate_configuration(),
1                        _generate_version_info(),
1                    )
1                )
1                AINodeDescriptor().get_config().set_ainode_id(ainode_id)
1                system_properties = _generate_system_properties(ainode_id)
1                with open(system_properties_file, "w") as f:
1                    f.write("#" + str(datetime.now()) + "\n")
1                    for key, value in system_properties.items():
1                        f.write(key + "=" + str(value) + "\n")
1            except Exception as e:
1                logger.error(
1                    "IoTDB-AINode failed to register to IoTDB cluster: {}".format(e)
1                )
1                raise e
1        else:
1            # If the system.properties file does exist, the AINode will just restart.
1            try:
1                logger.info("IoTDB-AINode is restarting...")
1                ClientManager().borrow_config_node_client().node_restart(
1                    AINodeDescriptor().get_config().get_cluster_name(),
1                    _generate_configuration(),
1                    _generate_version_info(),
1                )
1            except Exception as e:
1                logger.error("IoTDB-AINode failed to restart: {}".format(e))
1                raise e
1
1        # Start the RPC service
1        self._rpc_handler = AINodeRPCServiceHandler(ainode=self)
1        self._rpc_service = AINodeRPCService(self._rpc_handler)
1        self._rpc_service.start()
1        self._rpc_service.join(1)
1        if self._rpc_service.exit_code != 0:
1            logger.info("IoTDB-AINode failed to start, please check previous logs.")
1            return
1
1        logger.info("IoTDB-AINode has successfully started.")
1
1        # Register stop hook
1        self._stop_event = threading.Event()
1        signal.signal(signal.SIGTERM, self._handle_signal)
1
1        self._rpc_service.join()
1
1    def _handle_signal(self, signum, frame):
1        signal_name = {signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}.get(
1            signum, f"SIGNAL {signum}"
1        )
1
1        logger.info(f"IoTDB-AINode receives {signal_name}, initiating graceful stop...")
1        self.stop()
1
1    def stop(self):
1        if not self._stop_event.is_set():
1            self._stop_event.set()
1            self._rpc_handler.stop()
1            if self._rpc_service:
1                self._rpc_service.stop()
1                self._rpc_service.join(1)
1                if self._rpc_service.is_alive():
1                    logger.warning("RPC service thread failed to stop in time.")
1            logger.info("IoTDB-AINode has successfully stopped.")
1