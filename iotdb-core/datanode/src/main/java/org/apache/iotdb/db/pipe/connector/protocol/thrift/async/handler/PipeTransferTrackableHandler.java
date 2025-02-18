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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler;

import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

public abstract class PipeTransferTrackableHandler
    implements AsyncMethodCallback<TPipeTransferResp>, AutoCloseable {

  protected final IoTDBDataRegionAsyncConnector connector;

  public PipeTransferTrackableHandler(final IoTDBDataRegionAsyncConnector connector) {
    this.connector = connector;
  }

  @Override
  public void onComplete(final TPipeTransferResp response) {
    if (connector.isClosed()) {
      clearEventsReferenceCount();
      connector.eliminateHandler(this);
      return;
    }

    if (onCompleteInternal(response)) {
      // eliminate handler only when all transmissions corresponding to the handler have been
      // completed
      // NOTE: We should not clear the reference count of events, as this would cause the
      // `org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic.IoTDBPipeDataSinkIT#testSinkTsFileFormat3` test to fail.
      connector.eliminateHandler(this);
    }
  }

  @Override
  public void onError(final Exception exception) {
    if (connector.isClosed()) {
      clearEventsReferenceCount();
      connector.eliminateHandler(this);
      return;
    }

    onErrorInternal(exception);
    connector.eliminateHandler(this);
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
    // track handler before checking if connector is closed
    connector.trackHandler(this);
    if (connector.isClosed()) {
      clearEventsReferenceCount();
      connector.eliminateHandler(this);
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

  @Override
  public void close() {
    // do nothing
  }
}
