package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

public abstract class MockClientManager implements IClientManager {

  RaftService.AsyncClient asyncClient;
  RaftService.Client syncClient;

  public void setAsyncClient(RaftService.AsyncClient asyncClient) {
    this.asyncClient = asyncClient;
  }

  public void setSyncClient(RaftService.Client client) {
    this.syncClient = client;
  }

  @Override
  public RaftService.AsyncClient borrowAsyncClient(Node node, ClientCategory category) {
    return null;
  }

  @Override
  public RaftService.Client borrowSyncClient(Node node, ClientCategory category) {
    return null;
  }
}
