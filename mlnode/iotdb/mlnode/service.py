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
import time

from thrift.protocol import TCompactProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from iotdb.mlnode.config import config
from iotdb.mlnode.handler import MLNodeRPCServiceHandler
from iotdb.mlnode.log import logger
from iotdb.thrift.mlnode import IMLNodeRPCService


class RPCService(threading.Thread):
    def __init__(self):
        super().__init__()
        processor = IMLNodeRPCService.Processor(handler=MLNodeRPCServiceHandler())
        transport = TSocket.TServerSocket(host=config.get_mn_rpc_address(), port=config.get_mn_rpc_port())
        transport_factory = TTransport.TBufferedTransportFactory()
        protocol_factory = TCompactProtocol.TCompactProtocolFactory()

        self.__pool_server = TServer.TThreadPoolServer(processor, transport, transport_factory, protocol_factory)

    def run(self) -> None:
        logger.info("The RPC service thread begin to run...")
        self.__pool_server.serve()


class MLNode(object):
    def __init__(self):
        self.__rpc_service = RPCService()

    def start(self) -> None:
        self.__rpc_service.start()

        # sleep 100ms for waiting the rpc server start.
        time.sleep(0.1)
        logger.info('IoTDB-MLNode has successfully started.')


if __name__ == "__main__":
    server = MLNode()
    server.start()
