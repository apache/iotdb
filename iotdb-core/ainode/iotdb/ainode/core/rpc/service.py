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
1import threading
1
1from thrift.protocol import TBinaryProtocol, TCompactProtocol
1from thrift.server import TServer
1from thrift.transport import TSocket, TSSLSocket, TTransport
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
1from iotdb.thrift.ainode import IAINodeRPCService
1
1logger = Logger()
1
1
1class AINodeThreadPoolServer(TServer.TThreadPoolServer):
1    def __init__(self, *args, **kwargs):
1        super().__init__(*args, **kwargs)
1        self._stop_event = threading.Event()
1
1    def serve(self) -> None:
1        self._stop_event.clear()
1        logger.info("The RPC service thread pool of IoTDB-AINode begins to serve...")
1        """Start a fixed number of worker threads and put client into a queue"""
1        for i in range(self.threads):
1            try:
1                t = threading.Thread(target=self.serveThread)
1                t.daemon = self.daemon
1                t.start()
1            except Exception as x:
1                logger.error(x)
1        # Pump the socket for clients
1        self.serverTransport.listen()
1        while not self._stop_event.is_set():
1            try:
1                client = self.serverTransport.accept()  # TODO: Fix the block problem
1                if not client:
1                    continue
1                self.clients.put(client)
1            except Exception as x:
1                logger.error(x)
1        logger.info(
1            "The RPC service thread pool of IoTDB-AINode has successfully stopped."
1        )
1
1    def stop(self) -> None:
1        if not self._stop_event.is_set():
1            logger.info("Stopping the RPC service thread pool of IoTDB-AINode...")
1            self._stop_event.set()
1            self.serverTransport.close()
1
1
1class AINodeRPCService(threading.Thread):
1    def __init__(self, handler: AINodeRPCServiceHandler):
1        super().__init__()
1        self.exit_code = 0
1        self._stop_event = threading.Event()
1        self._handler = handler
1        processor = IAINodeRPCService.Processor(handler=self._handler)
1        if AINodeDescriptor().get_config().get_ain_internal_ssl_enabled():
1            import ssl
1            import sys
1
1            if sys.version_info >= (3, 10):
1                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
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
1            transport = TSSLSocket.TSSLServerSocket(
1                host=AINodeDescriptor().get_config().get_ain_rpc_address(),
1                port=AINodeDescriptor().get_config().get_ain_rpc_port(),
1                ssl_context=context,
1            )
1        else:
1            transport = TSocket.TServerSocket(
1                host=AINodeDescriptor().get_config().get_ain_rpc_address(),
1                port=AINodeDescriptor().get_config().get_ain_rpc_port(),
1            )
1        transport_factory = TTransport.TFramedTransportFactory()
1        if AINodeDescriptor().get_config().get_ain_thrift_compression_enabled():
1            protocol_factory = TCompactProtocol.TCompactProtocolFactory()
1        else:
1            protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()
1        # Create daemon thread pool server
1        self.__pool_server = AINodeThreadPoolServer(
1            processor, transport, transport_factory, protocol_factory, daemon=True
1        )
1
1    def run(self) -> None:
1        logger.info("The RPC service of IoTDB-AINode begins to run...")
1        try:
1            self.__pool_server.serve()
1        except Exception as e:
1            self.exit_code = 1
1            logger.error(e)
1        finally:
1            logger.info("The RPC service of IoTDB-AINode exited.")
1
1    def stop(self) -> None:
1        if not self._stop_event.is_set():
1            logger.info("Stopping the RPC service of IoTDB-AINode...")
1            self._stop_event.set()
1            self.__pool_server.stop()
1