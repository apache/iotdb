/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.client.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.AsyncThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.IoTConsensusV2IService;
import org.apache.iotdb.rpc.TNonblockingTransportWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.tsfile.external.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncIoTConsensusV2ServiceClient extends IoTConsensusV2IService.AsyncClient
    implements ThriftClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncIoTConsensusV2ServiceClient.class);
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private static final AtomicInteger idGenerator = new AtomicInteger(0);
  private final int id = idGenerator.incrementAndGet();
  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncIoTConsensusV2ServiceClient> clientManager;
  private final AtomicBoolean shouldReturnSelf = new AtomicBoolean(true);

  public AsyncIoTConsensusV2ServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      TAsyncClientManager tAsyncClientManager,
      ClientManager<TEndPoint, AsyncIoTConsensusV2ServiceClient> clientManager)
      throws IOException {
    super(
        property.getProtocolFactory(),
        tAsyncClientManager,
        commonConfig.isEnableInternalSSL()
            ? TNonblockingTransportWrapper.wrap(
                endpoint.getIp(),
                endpoint.getPort(),
                property.getConnectionTimeoutMs(),
                commonConfig.getKeyStorePath(),
                commonConfig.getKeyStorePwd(),
                commonConfig.getTrustStorePath(),
                commonConfig.getTrustStorePwd())
            : TNonblockingTransportWrapper.wrap(
                endpoint.getIp(), endpoint.getPort(), property.getConnectionTimeoutMs()));
    setTimeout(property.getConnectionTimeoutMs());
    this.printLogWhenEncounterException = property.isPrintLogWhenEncounterException();
    this.endpoint = endpoint;
    this.clientManager = clientManager;
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
  }

  @Override
  public void onError(Exception e) {
    super.onError(e);
    ThriftClient.resolveException(e, this);
    returnSelf();
  }

  @Override
  public void invalidate() {
    if (!hasError()) {
      super.onError(new Exception(String.format("This client %d has been invalidated", id)));
    }
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endpoint);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  public void returnSelf() {
    if (shouldReturnSelf.get()) {
      clientManager.returnClient(endpoint, this);
    }
  }

  public void setShouldReturnSelf(boolean shouldReturnSelf) {
    this.shouldReturnSelf.set(shouldReturnSelf);
  }

  private void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      LOGGER.info(
          "Unexpected exception occurs in {}, error msg is {}",
          this,
          ExceptionUtils.getRootCause(e).toString());
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("AsyncPipeDataTransferServiceClient{%s}, id = {%d}", endpoint, id);
  }

  public TEndPoint getTEndpoint() {
    return endpoint;
  }

  public static class Factory
      extends AsyncThriftClientFactory<TEndPoint, AsyncIoTConsensusV2ServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncIoTConsensusV2ServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        String threadName) {
      super(clientManager, thriftClientProperty, threadName);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncIoTConsensusV2ServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncIoTConsensusV2ServiceClient> makeObject(TEndPoint endPoint)
        throws Exception {
      return new DefaultPooledObject<>(
          new AsyncIoTConsensusV2ServiceClient(
              thriftClientProperty,
              endPoint,
              tManagers[clientCnt.incrementAndGet() % tManagers.length],
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncIoTConsensusV2ServiceClient> pooledObject) {
      return pooledObject.getObject().isReady();
    }
  }
}
