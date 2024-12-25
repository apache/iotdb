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
import threading

from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from iotdb.ainode.config import AINodeDescriptor
from iotdb.ainode.handler import AINodeRPCServiceHandler
from iotdb.ainode.log import Logger
from iotdb.thrift.ainode import IAINodeRPCService

logger = Logger()


class RPCService(threading.Thread):
    def __init__(self):
        self.exit_code = 0
        super().__init__()
        processor = IAINodeRPCService.Processor(handler=AINodeRPCServiceHandler())
        transport = TSocket.TServerSocket(host=AINodeDescriptor().get_config().get_ain_inference_rpc_address(),
                                          port=AINodeDescriptor().get_config().get_ain_inference_rpc_port())
        transport_factory = TTransport.TFramedTransportFactory()
        if AINodeDescriptor().get_config().get_ain_thrift_compression_enabled():
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
