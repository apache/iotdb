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
import time

from thrift.Thrift import TException
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.transport import TSocket, TTransport

from iotdb.ainode.config import AINodeDescriptor
from iotdb.ainode.constant import TSStatusCode
from iotdb.ainode.log import Logger
from iotdb.ainode.util.decorator import singleton
from iotdb.ainode.util.status import verify_success
from iotdb.thrift.common.ttypes import TEndPoint, TSStatus, TAINodeLocation, TAINodeConfiguration
from iotdb.thrift.confignode import IConfigNodeRPCService
from iotdb.thrift.confignode.ttypes import (TAINodeRemoveReq, TNodeVersionInfo,
                                            TAINodeRegisterReq, TAINodeRestartReq)

logger = Logger()


@singleton
class ClientManager(object):
    def __init__(self):
        self._config_node_endpoint = AINodeDescriptor().get_config().get_ain_target_config_node_list()

    def borrow_config_node_client(self):
        return ConfigNodeClient(config_leader=self._config_node_endpoint)


class ConfigNodeClient(object):
    def __init__(self, config_leader: TEndPoint):
        self._config_leader = config_leader
        self._config_nodes = []
        self._cursor = 0
        self._transport = None
        self._client = None

        self._MSG_RECONNECTION_FAIL = "Fail to connect to any config node. Please check status of ConfigNodes"
        self._RETRY_NUM = 5
        self._RETRY_INTERVAL_MS = 1

        self._try_to_connect()

    def _try_to_connect(self) -> None:
        if self._config_leader is not None:
            try:
                self._connect(self._config_leader)
                return
            except TException:
                logger.warning("The current node {} may have been down, try next node", self._config_leader)
                self._config_leader = None

        if self._transport is not None:
            self._transport.close()

        try_host_num = 0
        while try_host_num < len(self._config_nodes):
            self._cursor = (self._cursor + 1) % len(self._config_nodes)

            try_endpoint = self._config_nodes[self._cursor]
            try:
                self._connect(try_endpoint)
                return
            except TException:
                logger.warning("The current node {} may have been down, try next node", try_endpoint)

            try_host_num = try_host_num + 1

        raise TException(self._MSG_RECONNECTION_FAIL)

    def _connect(self, target_config_node: TEndPoint) -> None:
        transport = TTransport.TFramedTransport(
            TSocket.TSocket(target_config_node.ip, target_config_node.port)
        )
        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                logger.error("TTransportException: {}".format(e))
                raise e

        if AINodeDescriptor().get_config().get_ain_thrift_compression_enabled():
            protocol = TCompactProtocol.TCompactProtocol(transport)
        else:
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self._client = IConfigNodeRPCService.Client(protocol)

    def _wait_and_reconnect(self) -> None:
        # wait to start the next try
        time.sleep(self._RETRY_INTERVAL_MS)

        try:
            self._try_to_connect()
        except TException:
            # can not connect to each config node
            self._sync_latest_config_node_list()
            self._try_to_connect()

    def _sync_latest_config_node_list(self) -> None:
        # TODO
        pass

    def _update_config_node_leader(self, status: TSStatus) -> bool:
        if status.code == TSStatusCode.REDIRECTION_RECOMMEND.get_status_code():
            if status.redirectNode is not None:
                self._config_leader = status.redirectNode
            else:
                self._config_leader = None
            return True
        return False

    def node_register(self, cluster_name: str, configuration: TAINodeConfiguration,
                      version_info: TNodeVersionInfo) -> int:
        req = TAINodeRegisterReq(
            clusterName=cluster_name,
            aiNodeConfiguration=configuration,
            versionInfo=version_info
        )

        for _ in range(0, self._RETRY_NUM):
            try:
                resp = self._client.registerAINode(req)
                if not self._update_config_node_leader(resp.status):
                    verify_success(resp.status, "An error occurs when calling node_register()")
                    self._config_nodes = resp.configNodeList
                    return resp.aiNodeId
            except TTransport.TException:
                logger.warning("Failed to connect to ConfigNode {} from AINode when executing node_register()",
                               self._config_leader)
                self._config_leader = None
            self._wait_and_reconnect()

        raise TException(self._MSG_RECONNECTION_FAIL)

    def node_restart(self, cluster_name: str, configuration: TAINodeConfiguration,
                     version_info: TNodeVersionInfo) -> None:
        req = TAINodeRestartReq(
            clusterName=cluster_name,
            aiNodeConfiguration=configuration,
            versionInfo=version_info
        )

        for _ in range(0, self._RETRY_NUM):
            try:
                resp = self._client.restartAINode(req)
                if not self._update_config_node_leader(resp.status):
                    verify_success(resp.status, "An error occurs when calling node_restart()")
                    self._config_nodes = resp.configNodeList
                    return resp.status
            except TTransport.TException:
                logger.warning("Failed to connect to ConfigNode {} from AINode when executing node_restart()",
                               self._config_leader)
                self._config_leader = None
            self._wait_and_reconnect()

        raise TException(self._MSG_RECONNECTION_FAIL)

    def node_remove(self, location: TAINodeLocation):
        req = TAINodeRemoveReq(
            aiNodeLocation=location
        )
        for _ in range(0, self._RETRY_NUM):
            try:
                status = self._client.removeAINode(req)
                if not self._update_config_node_leader(status):
                    verify_success(status, "An error occurs when calling node_restart()")
                    return status
            except TTransport.TException:
                logger.warning("Failed to connect to ConfigNode {} from AINode when executing node_remove()",
                               self._config_leader)
                self._config_leader = None
            self._wait_and_reconnect()
        raise TException(self._MSG_RECONNECTION_FAIL)

    def get_ainode_configuration(self, node_id: int) -> map:
        for _ in range(0, self._RETRY_NUM):
            try:
                resp = self._client.getAINodeConfiguration(node_id)
                if not self._update_config_node_leader(resp.status):
                    verify_success(resp.status, "An error occurs when calling get_ainode_configuration()")
                    return resp.aiNodeConfigurationMap
            except TTransport.TException:
                logger.warning("Failed to connect to ConfigNode {} from AINode when executing "
                               "get_ainode_configuration()",
                               self._config_leader)
                self._config_leader = None
            self._wait_and_reconnect()
        raise TException(self._MSG_RECONNECTION_FAIL)
