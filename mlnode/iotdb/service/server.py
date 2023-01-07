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


from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from iotdb.thrift.mlnode.IMLNodeRPCService import (
    Processor,
)

from iotdb.conf.config import *

from .rpchandler import IMLNodeRPCServiceHandler


class MLNodeServer(object):
    def __init__(self, host=ML_HOST, port=ML_PORT):
        self.host, self.port = host, port
        handler = IMLNodeRPCServiceHandler()
        processor = Processor(handler)
        transport = TSocket.TServerSocket(self.host, self.port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        self.server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)


    def serve(self):
        print('Starting ml server at', self.host,':', self.port)
        self.server.serve()
        print('done!')
