package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AsyncBaseFactory<K, T extends RaftService.AsyncClient>
    implements KeyedPooledObjectFactory<K, T> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncBaseFactory.class);

  // TODO: check what the managers are doing
  protected TAsyncClientManager[] managers;
  protected TProtocolFactory protocolFactory;
  protected AtomicInteger clientCnt = new AtomicInteger();
  protected ClientCategory category;

  public AsyncBaseFactory(TProtocolFactory protocolFactory, ClientCategory category) {
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
    this.protocolFactory = protocolFactory;
    this.category = category;
  }

  @Override
  public void activateObject(K node, PooledObject<T> pooledObject) throws Exception {}

  @Override
  public void passivateObject(K node, PooledObject<T> pooledObject) throws Exception {}
}
