package org.apache.iotdb.cluster.client;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseFactory<K, T> implements KeyedPooledObjectFactory<K, T> {

  private static final Logger logger = LoggerFactory.getLogger(BaseFactory.class);

  protected TAsyncClientManager[] managers;
  protected TProtocolFactory protocolFactory;
  protected AtomicInteger clientCnt = new AtomicInteger();
  protected ClientCategory category;
  protected IClientManager clientPoolManager;

  public BaseFactory(TProtocolFactory protocolFactory, ClientCategory category) {
    this.protocolFactory = protocolFactory;
    this.category = category;
  }

  public BaseFactory(
      TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
    this.protocolFactory = protocolFactory;
    this.category = category;
    this.clientPoolManager = clientManager;
  }

  @Override
  public void activateObject(K node, PooledObject<T> pooledObject) throws Exception {}

  @Override
  public void passivateObject(K node, PooledObject<T> pooledObject) throws Exception {}
}
