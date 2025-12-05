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
import org.apache.iotdb.rpc.TNonblockingTransportWrapper;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.tsfile.external.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class AsyncPipeDataTransferServiceClient extends IClientRPCService.AsyncClient
    implements ThriftClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncPipeDataTransferServiceClient.class);

  private static final AtomicInteger idGenerator = new AtomicInteger(0);
  private final int id = idGenerator.incrementAndGet();

  private boolean printLogWhenEncounterException;

  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager;

  private final AtomicBoolean shouldReturnSelf = new AtomicBoolean(true);

  private final AtomicBoolean isHandshakeFinished = new AtomicBoolean(false);

  public AsyncPipeDataTransferServiceClient(
      final ThriftClientProperty property,
      final TEndPoint endpoint,
      final TAsyncClientManager tClientManager,
      final ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager)
      throws IOException {
    super(
        property.getProtocolFactory(),
        tClientManager,
        TNonblockingTransportWrapper.wrap(
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
  public void onError(final Exception e) {
    super.onError(e);
    ThriftClient.resolveException(e, this);
    setPrintLogWhenEncounterException(false);
    returnSelf(
        (i) -> i instanceof IllegalStateException && "Client has an error!".equals(i.getMessage()));
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

  public void setPrintLogWhenEncounterException(final boolean printLogWhenEncounterException) {
    this.printLogWhenEncounterException = printLogWhenEncounterException;
  }

  /**
   * return self, the method doesn't need to be called by the user and will be triggered after the
   * RPC is finished.
   */
  public void returnSelf() {
    if (shouldReturnSelf.get()) {
      clientManager.returnClient(endpoint, this);
    }
  }

  /**
   * return self, the method doesn't need to be called by the user and will be triggered after the
   * RPC is finished.
   */
  public void returnSelf(Function<Exception, Boolean> ignoreError) {
    if (shouldReturnSelf.get()) {
      clientManager.returnClient(endpoint, this, ignoreError);
    }
  }

  public void setShouldReturnSelf(final boolean shouldReturnSelf) {
    this.shouldReturnSelf.set(shouldReturnSelf);
  }

  public void setTimeoutDynamically(final int timeout) {
    try {
      ((TNonblockingSocket) ___transport).setTimeout(timeout);
    } catch (Exception e) {
      setTimeout(timeout);
      LOGGER.error("Failed to set timeout dynamically, set it statically", e);
    }
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  private boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      if (printLogWhenEncounterException) {
        LOGGER.error(
            "Unexpected exception occurs in {}, error msg is {}",
            this,
            ExceptionUtils.getRootCause(e).toString(),
            e);
      }
      return false;
    }
  }

  public boolean isHandshakeFinished() {
    return isHandshakeFinished.get();
  }

  public void markHandshakeFinished() {
    isHandshakeFinished.set(true);
    LOGGER.info("Handshake finished for client {}", this);
  }

  // To ensure that the socket will be closed eventually, we need to manually close the socket here,
  // because the Client object may have thrown an exception before entering the asynchronous thread,
  // and the returnSelf method may not be called, resulting in resource leakage.
  public void resetMethodStateIfStopped() {
    if (!___manager.isRunning()) {
      if (___transport != null && ___transport.isOpen()) {
        ___transport.close();
        LOGGER.warn("Manually closing transport to prevent resource leakage.");
      }
      ___currentMethod = null;
      LOGGER.info("Method state has been reset due to manager not running.");
    }
  }

  public String getIp() {
    return endpoint.getIp();
  }

  public int getPort() {
    return endpoint.getPort();
  }

  public TEndPoint getEndPoint() {
    return endpoint;
  }

  @Override
  public String toString() {
    return String.format("AsyncPipeDataTransferServiceClient{%s}, id = {%d}", endpoint, id);
  }

  public static class Factory
      extends AsyncThriftClientFactory<TEndPoint, AsyncPipeDataTransferServiceClient> {

    public Factory(
        final ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager,
        final ThriftClientProperty thriftClientProperty,
        final String threadName) {
      super(clientManager, thriftClientProperty, threadName);
    }

    @Override
    public void destroyObject(
        final TEndPoint endPoint,
        final PooledObject<AsyncPipeDataTransferServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncPipeDataTransferServiceClient> makeObject(final TEndPoint endPoint)
        throws Exception {
      return new DefaultPooledObject<>(
          new AsyncPipeDataTransferServiceClient(
              thriftClientProperty,
              endPoint,
              tManagers[clientCnt.incrementAndGet() % tManagers.length],
              clientManager));
    }

    @Override
    public boolean validateObject(
        final TEndPoint endPoint,
        final PooledObject<AsyncPipeDataTransferServiceClient> pooledObject) {
      return pooledObject.getObject().isReady();
    }
  }
}
