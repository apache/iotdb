package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class TimeoutChangeableTFastFramedTransport extends TFastFramedTransport {

  TTransport underlying;

  public TimeoutChangeableTFastFramedTransport(TTransport underlying) {
    super(underlying);
    this.underlying = underlying;
  }

  public void setTimeout(int timeout){
    ((TSocket)underlying).setTimeout(timeout);
  }
}
