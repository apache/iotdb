package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

public interface IClientPool {
  RaftService.AsyncClient borrowAsyncClient(Node node, ClientCategory category);

  RaftService.Client borrowSyncClient(Node node, ClientCategory category);

  void returnAsyncClient(RaftService.AsyncClient client, Node node, ClientCategory category);

  void returnSyncClient(RaftService.Client client, Node node, ClientCategory category);
}
