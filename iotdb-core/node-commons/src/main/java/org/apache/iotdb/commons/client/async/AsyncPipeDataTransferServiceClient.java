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
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncPipeDataTransferServiceClient extends IClientRPCService.AsyncClient
    implements ThriftClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncPipeDataTransferServiceClient.class);

  private static final AtomicInteger idGenerator = new AtomicInteger(0);
  private final int id = idGenerator.incrementAndGet();

  private final boolean printLogWhenEncounterException;

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
        TNonblockingSocketWrapper.wrap(
            endpoint.getIp(), endpoint.getPort(), property.getConnectionTimeoutMs()));
    setTimeout(property.getConnectionTimeoutMs());
    this.printLogWhenEncounterException = property.isPrintLogWhenEncounterException();
    this.endpoint = endpoint;
    this.clientManager = clientManager;
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, constructor", id);
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, onComplete", id);
  }

  @Override
  public void onError(final Exception e) {
    super.onError(e);
    ThriftClient.resolveException(e, this);
    returnSelf();
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, onError", id, e);
  }

  @Override
  public void invalidate() {
    if (!hasError()) {
      super.onError(new Exception(String.format("This client %d has been invalidated", id)));
      LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, invalidate 1", id);
    }
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, invalidate 2", id, new Exception());
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endpoint);
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, invalidateAll", id, new Exception());
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  /**
   * return self, the method doesn't need to be called by the user and will be triggered after the
   * RPC is finished.
   */
  public void returnSelf() {
    if (shouldReturnSelf.get()) {
      if (clientManager.isClosed()) {
        this.close();
        this.invalidateAll();
      }
      clientManager.returnClient(endpoint, this);
      LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, returnSelf 1", id);
    }
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, returnSelf 2", id);
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
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, timeout dynamically", id);
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, closed", id);
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
        LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, resetMethodState 1", id);
      }
      ___currentMethod = null;
      LOGGER.info("Method state has been reset due to manager not running.");
      LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, resetMethodState 2", id);
    }
    LOGGER.warn("AsyncPipeDataTransferServiceClient id = {}, resetMethodState 3", id);
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
      LOGGER.warn(
          "AsyncPipeDataTransferServiceClient#Factory endpoint = {}, destroyObject", endPoint);
    }

    @Override
    public PooledObject<AsyncPipeDataTransferServiceClient> makeObject(final TEndPoint endPoint)
        throws Exception {
      LOGGER.warn("AsyncPipeDataTransferServiceClient#Factory endpoint = {}, makeObject", endPoint);
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
      LOGGER.warn(
          "AsyncPipeDataTransferServiceClient#Factory endpoint = {},   validateObject", endPoint);
      return pooledObject.getObject().isReady();
    }
  }
}
