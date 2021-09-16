package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.client.BaseFactory;
import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.IClientManager;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AsyncBaseFactory<K, T extends RaftService.AsyncClient>
    extends BaseFactory<K, T> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncBaseFactory.class);

  public AsyncBaseFactory(TProtocolFactory protocolFactory, ClientCategory category) {
    super(protocolFactory, category);
    managers =
        new TAsyncClientManager
            [ClusterDescriptor.getInstance().getConfig().getSelectorNumOfClientPool()];
    for (int i = 0; i < managers.length; i++) {
      try {
        managers[i] = new TAsyncClientManager();
      } catch (IOException e) {
        logger.error("Cannot create data heartbeat client manager for factory", e);
      }
    }
  }

  public AsyncBaseFactory(
      TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
    super(protocolFactory, category, clientManager);
    managers =
        new TAsyncClientManager
            [ClusterDescriptor.getInstance().getConfig().getSelectorNumOfClientPool()];
    for (int i = 0; i < managers.length; i++) {
      try {
        managers[i] = new TAsyncClientManager();
      } catch (IOException e) {
        logger.error("Cannot create data heartbeat client manager for factory", e);
      }
    }
  }
}
