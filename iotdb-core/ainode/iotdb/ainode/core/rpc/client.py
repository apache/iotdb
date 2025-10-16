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
1import time
1
1from thrift.protocol import TBinaryProtocol, TCompactProtocol
1from thrift.Thrift import TException
1from thrift.transport import TSocket, TSSLSocket, TTransport
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import TSStatusCode
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.rpc.status import verify_success
1from iotdb.ainode.core.util.decorator import singleton
1from iotdb.thrift.common.ttypes import (
1    TAINodeConfiguration,
1    TAINodeLocation,
1    TEndPoint,
1    TSStatus,
1)
1from iotdb.thrift.confignode import IConfigNodeRPCService
1from iotdb.thrift.confignode.ttypes import (
1    TAINodeRegisterReq,
1    TAINodeRemoveReq,
1    TAINodeRestartReq,
1    TNodeVersionInfo,
1    TUpdateModelInfoReq,
1)
1
1logger = Logger()
1
1
1@singleton
1class ClientManager(object):
1    def __init__(self):
1        self._config_node_endpoint = (
1            AINodeDescriptor().get_config().get_ain_target_config_node_list()
1        )
1
1    def borrow_config_node_client(self):
1        return ConfigNodeClient(config_leader=self._config_node_endpoint)
1
1
1class ConfigNodeClient(object):
1    def __init__(self, config_leader: TEndPoint):
1        self._config_leader = config_leader
1        self._config_nodes = []
1        self._cursor = 0
1        self._transport = None
1        self._client = None
1
1        self._MSG_RECONNECTION_FAIL = (
1            "Fail to connect to any config node. Please check status of ConfigNodes"
1        )
1        self._RETRY_NUM = 10
1        self._RETRY_INTERVAL_IN_S = 1
1
1        self._try_to_connect()
1
1    def _try_to_connect(self) -> None:
1        if self._config_leader is not None:
1            try:
1                self._connect(self._config_leader)
1                return
1            except TException:
1                logger.warning(
1                    "The current node {} may have been down, try next node",
1                    self._config_leader,
1                )
1                self._config_leader = None
1
1        if self._transport is not None:
1            self._transport.close()
1
1        try_host_num = 0
1        while try_host_num < len(self._config_nodes):
1            self._cursor = (self._cursor + 1) % len(self._config_nodes)
1            try_endpoint = self._config_nodes[self._cursor]
1            for _ in range(0, self._RETRY_NUM):
1                try:
1                    self._connect(try_endpoint)
1                    return
1                except TException:
1                    logger.warning(
1                        "The current node {} may have been down, waiting and retry...",
1                        try_endpoint,
1                    )
1                    time.sleep(self._RETRY_INTERVAL_IN_S)
1            logger.warning(
1                "The current node {} may have been down, try next node...",
1                try_endpoint,
1            )
1            try_host_num = try_host_num + 1
1
1        raise TException(self._MSG_RECONNECTION_FAIL)
1
1    def _connect(self, target_config_node: TEndPoint) -> None:
1        if AINodeDescriptor().get_config().get_ain_internal_ssl_enabled():
1            import ssl
1            import sys
1
1            if sys.version_info >= (3, 10):
1                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
1            else:
1                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
1                context.verify_mode = ssl.CERT_REQUIRED
1            context.check_hostname = False
1            context.load_verify_locations(
1                cafile=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file()
1            )
1            context.load_cert_chain(
1                certfile=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
1                keyfile=AINodeDescriptor().get_config().get_ain_thrift_ssl_key_file(),
1            )
1            socket = TSSLSocket.TSSLSocket(
1                host=target_config_node.ip,
1                port=target_config_node.port,
1                ssl_context=context,
1            )
1        else:
1            socket = TSocket.TSocket(target_config_node.ip, target_config_node.port)
1        transport = TTransport.TFramedTransport(socket)
1        if not transport.isOpen():
1            try:
1                transport.open()
1            except TTransport.TTransportException as e:
1                logger.error("TTransportException: {}".format(e))
1                raise e
1
1        if AINodeDescriptor().get_config().get_ain_thrift_compression_enabled():
1            protocol = TCompactProtocol.TCompactProtocol(transport)
1        else:
1            protocol = TBinaryProtocol.TBinaryProtocol(transport)
1        self._client = IConfigNodeRPCService.Client(protocol)
1
1    def _wait_and_reconnect(self) -> None:
1        # wait to start the next try
1        time.sleep(self._RETRY_INTERVAL_IN_S)
1
1        try:
1            self._try_to_connect()
1        except TException:
1            # can not connect to each config node
1            self._sync_latest_config_node_list()
1            self._try_to_connect()
1
1    def _sync_latest_config_node_list(self) -> None:
1        # TODO
1        pass
1
1    def _update_config_node_leader(self, status: TSStatus) -> bool:
1        if status.code == TSStatusCode.REDIRECTION_RECOMMEND.get_status_code():
1            if status.redirectNode is not None:
1                self._config_leader = status.redirectNode
1            else:
1                self._config_leader = None
1            return True
1        return False
1
1    def node_register(
1        self,
1        cluster_name: str,
1        configuration: TAINodeConfiguration,
1        version_info: TNodeVersionInfo,
1    ) -> int:
1        req = TAINodeRegisterReq(
1            clusterName=cluster_name,
1            aiNodeConfiguration=configuration,
1            versionInfo=version_info,
1        )
1
1        for _ in range(0, self._RETRY_NUM):
1            try:
1                resp = self._client.registerAINode(req)
1                if not self._update_config_node_leader(resp.status):
1                    verify_success(
1                        resp.status, "An error occurs when calling node_register()"
1                    )
1                    self._config_nodes = resp.configNodeList
1                    return resp.aiNodeId
1            except TTransport.TException:
1                logger.warning(
1                    "Failed to connect to ConfigNode {} from AINode when executing node_register()",
1                    self._config_leader,
1                )
1                self._config_leader = None
1            self._wait_and_reconnect()
1
1        raise TException(self._MSG_RECONNECTION_FAIL)
1
1    def node_restart(
1        self,
1        cluster_name: str,
1        configuration: TAINodeConfiguration,
1        version_info: TNodeVersionInfo,
1    ) -> None:
1        req = TAINodeRestartReq(
1            clusterName=cluster_name,
1            aiNodeConfiguration=configuration,
1            versionInfo=version_info,
1        )
1
1        for _ in range(0, self._RETRY_NUM):
1            try:
1                resp = self._client.restartAINode(req)
1                if not self._update_config_node_leader(resp.status):
1                    verify_success(
1                        resp.status, "An error occurs when calling node_restart()"
1                    )
1                    self._config_nodes = resp.configNodeList
1                    return resp.status
1            except TTransport.TException:
1                logger.warning(
1                    "Failed to connect to ConfigNode {} from AINode when executing node_restart()",
1                    self._config_leader,
1                )
1                self._config_leader = None
1            self._wait_and_reconnect()
1
1        raise TException(self._MSG_RECONNECTION_FAIL)
1
1    def node_remove(self, location: TAINodeLocation):
1        req = TAINodeRemoveReq(aiNodeLocation=location)
1        for _ in range(0, self._RETRY_NUM):
1            try:
1                status = self._client.removeAINode(req)
1                if not self._update_config_node_leader(status):
1                    verify_success(
1                        status, "An error occurs when calling node_restart()"
1                    )
1                    return status
1            except TTransport.TException:
1                logger.warning(
1                    "Failed to connect to ConfigNode {} from AINode when executing node_remove()",
1                    self._config_leader,
1                )
1                self._config_leader = None
1            self._wait_and_reconnect()
1        raise TException(self._MSG_RECONNECTION_FAIL)
1
1    def get_ainode_configuration(self, node_id: int) -> map:
1        for _ in range(0, self._RETRY_NUM):
1            try:
1                resp = self._client.getAINodeConfiguration(node_id)
1                if not self._update_config_node_leader(resp.status):
1                    verify_success(
1                        resp.status,
1                        "An error occurs when calling get_ainode_configuration()",
1                    )
1                    return resp.aiNodeConfigurationMap
1            except TTransport.TException:
1                logger.warning(
1                    "Failed to connect to ConfigNode {} from AINode when executing "
1                    "get_ainode_configuration()",
1                    self._config_leader,
1                )
1                self._config_leader = None
1            self._wait_and_reconnect()
1        raise TException(self._MSG_RECONNECTION_FAIL)
1
1    def update_model_info(
1        self,
1        model_id: str,
1        model_status: int,
1        attribute: str = "",
1        ainode_id=None,
1        input_length=0,
1        output_length=0,
1    ) -> None:
1        if ainode_id is None:
1            ainode_id = []
1        for _ in range(0, self._RETRY_NUM):
1            try:
1                req = TUpdateModelInfoReq(model_id, model_status, attribute)
1                if ainode_id is not None:
1                    req.aiNodeIds = ainode_id
1                req.inputLength = input_length
1                req.outputLength = output_length
1                status = self._client.updateModelInfo(req)
1                if not self._update_config_node_leader(status):
1                    verify_success(
1                        status, "An error occurs when calling update model info"
1                    )
1                    return status
1            except TTransport.TException:
1                logger.warning(
1                    "Failed to connect to ConfigNode {} from AINode when executing update model info",
1                    self._config_leader,
1                )
1                self._config_leader = None
1            self._wait_and_reconnect()
1        raise TException(self._MSG_RECONNECTION_FAIL)
1