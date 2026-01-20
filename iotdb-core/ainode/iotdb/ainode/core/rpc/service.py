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

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TSSLSocket, TTransport

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
from iotdb.thrift.ainode import IAINodeRPCService

logger = Logger()


class AINodeThreadPoolServer(TServer.TThreadPoolServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def serve(self) -> None:
        self._stop_event.clear()
        logger.info("The RPC service thread pool of IoTDB-AINode begins to serve...")
        """Start a fixed number of worker threads and put client into a queue"""
        for i in range(self.threads):
            try:
                t = threading.Thread(target=self.serveThread)
                t.daemon = self.daemon
                t.start()
            except Exception as x:
                logger.error(x)
        # Pump the socket for clients
        self.serverTransport.listen()
        while not self._stop_event.is_set():
            try:
                client = self.serverTransport.accept()
                if not client:
                    continue
                self.clients.put(client)
            except Exception as x:
                logger.error(x)
        logger.info(
            "The RPC service thread pool of IoTDB-AINode has successfully stopped."
        )

    def stop(self) -> None:
        if not self._stop_event.is_set():
            logger.info("Stopping the RPC service thread pool of IoTDB-AINode...")
            self._stop_event.set()
            self.serverTransport.close()


class AINodeRPCService(threading.Thread):
    def __init__(self, handler: AINodeRPCServiceHandler):
        super().__init__()
        self.exit_code = 0
        self._stop_event = threading.Event()
        self._handler = handler
        processor = IAINodeRPCService.Processor(handler=self._handler)
        if AINodeDescriptor().get_config().get_ain_internal_ssl_enabled():
            import ssl
            import sys

            if sys.version_info >= (3, 10):
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            else:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = False
            context.load_verify_locations(
                cafile=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file()
            )
            context.load_cert_chain(
                certfile=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
                keyfile=AINodeDescriptor().get_config().get_ain_thrift_ssl_key_file(),
            )
            transport = TSSLSocket.TSSLServerSocket(
                host=AINodeDescriptor().get_config().get_ain_rpc_address(),
                port=AINodeDescriptor().get_config().get_ain_rpc_port(),
                ssl_context=context,
            )
        else:
            transport = TSocket.TServerSocket(
                host=AINodeDescriptor().get_config().get_ain_rpc_address(),
                port=AINodeDescriptor().get_config().get_ain_rpc_port(),
            )
        transport_factory = TTransport.TFramedTransportFactory()
        if AINodeDescriptor().get_config().get_ain_thrift_compression_enabled():
            protocol_factory = TCompactProtocol.TCompactProtocolFactory()
        else:
            protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()
        # Create daemon thread pool server
        self.__pool_server = AINodeThreadPoolServer(
            processor, transport, transport_factory, protocol_factory, daemon=True
        )

    def run(self) -> None:
        logger.info("The RPC service of IoTDB-AINode begins to run...")
        try:
            self.__pool_server.serve()
        except Exception as e:
            self.exit_code = 1
            logger.error(e)
        finally:
            logger.info("The RPC service of IoTDB-AINode exited.")

    def stop(self) -> None:
        if not self._stop_event.is_set():
            logger.info("Stopping the RPC service of IoTDB-AINode...")
            self._stop_event.set()
            self.__pool_server.stop()
            self._handler.stop()
