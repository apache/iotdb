package org.apache.iotdb.db.newsync.transport.server;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

public class TransportServerThriftHandler implements TServerEventHandler {

  private TransportServiceImpl serviceImpl;

  public TransportServerThriftHandler(TransportServiceImpl serviceImpl) {
    this.serviceImpl = serviceImpl;
  }

  @Override
  public void preServe() {}

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    return null;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    // release query resources.
    serviceImpl.handleClientExit();
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {}
}
