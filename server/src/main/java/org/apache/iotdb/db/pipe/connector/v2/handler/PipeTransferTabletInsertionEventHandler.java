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

package org.apache.iotdb.db.pipe.connector.v2.handler;

import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.connector.v2.IoTDBThriftConnectorV2;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public abstract class PipeTransferTabletInsertionEventHandler<E extends TPipeTransferResp>
    implements AsyncMethodCallback<E> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeTransferInsertNodeTabletInsertionEventHandler.class);

  private final long requestCommitId;
  private final EnrichedEvent event;
  private final TPipeTransferReq req;

  private final IoTDBThriftConnectorV2 connector;

  private static final long MAX_RETRY_WAIT_TIME_MS =
      (long) (PipeConfig.getInstance().getPipeConnectorRetryIntervalMs() * Math.pow(2, 5));
  private int retryCount = 0;

  public PipeTransferTabletInsertionEventHandler(
      long requestCommitId,
      @Nullable EnrichedEvent event,
      TPipeTransferReq req,
      IoTDBThriftConnectorV2 connector) {
    this.requestCommitId = requestCommitId;
    this.event = event;
    this.req = req;
    this.connector = connector;

    Optional.ofNullable(event)
        .ifPresent(
            e -> e.increaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName()));
  }

  public void transfer(AsyncPipeDataTransferServiceClient client) throws TException {
    doTransfer(client, req);
  }

  protected abstract void doTransfer(
      AsyncPipeDataTransferServiceClient client, TPipeTransferReq req) throws TException;

  @Override
  public void onComplete(TPipeTransferResp response) {
    // just in case
    if (response == null) {
      onError(new PipeException("TPipeTransferResp is null"));
      return;
    }

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      connector.commit(requestCommitId, event);
    } else {
      onError(new PipeException(response.getStatus().getMessage()));
    }
  }

  @Override
  public void onError(Exception exception) {
    ++retryCount;

    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(
                Math.min(
                    (long)
                        (PipeConfig.getInstance().getPipeConnectorRetryIntervalMs()
                            * Math.pow(2, retryCount - 1)),
                    MAX_RETRY_WAIT_TIME_MS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Unexpected interruption during retrying", e);
          }

          if (connector.isClosed()) {
            LOGGER.info(
                "IoTDBThriftConnectorV2 has been stopped, we will not retry this request {} after {} times",
                req,
                retryCount,
                exception);
          } else {
            LOGGER.warn(
                "IoTDBThriftConnectorV2 failed to transfer request {} after {} times, retrying...",
                req,
                retryCount,
                exception);

            retryTransfer(connector, requestCommitId);
          }
        });
  }

  protected abstract void retryTransfer(IoTDBThriftConnectorV2 connector, long requestCommitId);
}
