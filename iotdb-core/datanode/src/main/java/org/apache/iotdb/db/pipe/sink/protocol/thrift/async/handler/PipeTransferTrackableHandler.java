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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class PipeTransferTrackableHandler
    implements AsyncMethodCallback<TPipeTransferResp>, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferTsFileHandler.class);

  protected final IoTDBDataRegionAsyncSink connector;
  protected volatile AsyncPipeDataTransferServiceClient client;

  public PipeTransferTrackableHandler(final IoTDBDataRegionAsyncSink connector) {
    this.connector = connector;
  }

  @Override
  public void onComplete(final TPipeTransferResp response) {
    if (connector.isClosed()) {
      clearEventsReferenceCount();
      connector.eliminateHandler(this, true);
      return;
    }

    if (onCompleteInternal(response)) {
      // eliminate handler only when all transmissions corresponding to the handler have been
      // completed
      // NOTE: We should not clear the reference count of events, as this would cause the
      // `org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic.IoTDBPipeDataSinkIT#testSinkTsFileFormat3` test to fail.
      connector.eliminateHandler(this, false);
    }
  }

  @Override
  public void onError(final Exception exception) {
    if (client != null) {
      ThriftClient.resolveException(exception, client);
      client.setPrintLogWhenEncounterException(false);
    }

    if (connector.isClosed()) {
      clearEventsReferenceCount();
      connector.eliminateHandler(this, true);
      return;
    }

    onErrorInternal(exception);
    connector.eliminateHandler(this, false);
  }

  /**
   * Attempts to transfer data using the provided client and request.
   *
   * @param client the client used for data transfer
   * @param req the request containing transfer details
   * @return {@code true} if the transfer was initiated successfully, {@code false} if the connector
   *     is closed
   * @throws TException if an error occurs during the transfer
   */
  protected boolean tryTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException {
    if (Objects.isNull(this.client)) {
      this.client = client;
    }
    // track handler before checking if connector is closed
    connector.trackHandler(this);
    if (connector.isClosed()) {
      clearEventsReferenceCount();
      connector.eliminateHandler(this, true);
      client.setShouldReturnSelf(true);
      client.returnSelf(
          (e) -> {
            if (e instanceof IllegalStateException) {
              LOGGER.info(
                  "Illegal state when return the client to object pool, maybe the pool is already cleared. Will ignore.");
              return true;
            }
            return false;
          });
      this.client = null;
      return false;
    }
    doTransfer(client, req);
    return true;
  }

  /**
   * @return {@code true} if all transmissions corresponding to the handler have been completed,
   *     {@code false} otherwise
   */
  protected abstract boolean onCompleteInternal(final TPipeTransferResp response);

  protected abstract void onErrorInternal(final Exception exception);

  protected abstract void doTransfer(
      final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
      throws TException;

  public abstract void clearEventsReferenceCount();

  public void closeClient() {
    if (Objects.isNull(client)) {
      return;
    }
    try {
      client.close();
      client.invalidateAll();
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to close or invalidate client when connector is closed. Client: {}, Exception: {}",
          client,
          e.getMessage(),
          e);
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}
