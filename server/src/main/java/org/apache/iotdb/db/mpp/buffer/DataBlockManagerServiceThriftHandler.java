package org.apache.iotdb.db.mpp.buffer;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

public class DataBlockManagerServiceThriftHandler implements TServerEventHandler {

  @Override
  public void preServe() {}

  @Override
  public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
    return null;
  }

  @Override
  public void deleteContext(
      ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {}

  @Override
  public void processContext(
      ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {}
}
