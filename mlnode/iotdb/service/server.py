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
