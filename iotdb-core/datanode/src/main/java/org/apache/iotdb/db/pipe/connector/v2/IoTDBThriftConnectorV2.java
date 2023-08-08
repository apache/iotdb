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

package org.apache.iotdb.db.pipe.connector.v2;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.base.IoTDBThriftConnector;
import org.apache.iotdb.db.pipe.connector.v1.IoTDBThriftConnectorV1;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.pipe.connector.v2.handler.PipeTransferInsertNodeTabletInsertionEventHandler;
import org.apache.iotdb.db.pipe.connector.v2.handler.PipeTransferRawTabletInsertionEventHandler;
import org.apache.iotdb.db.pipe.connector.v2.handler.PipeTransferTsFileInsertionEventHandler;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IoTDBThriftConnectorV2 extends IoTDBThriftConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftConnectorV2.class);

  private static final String FAILED_TO_BORROW_CLIENT_FORMATTER =
      "Failed to borrow client from client pool for receiver %s:%s.";

  private static final AtomicReference<
          IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>>
      ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER = new AtomicReference<>();
  private final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>
      asyncPipeDataTransferClientManager;

  private static final AtomicReference<ScheduledExecutorService> RETRY_TRIGGER =
      new AtomicReference<>();
  private static final int RETRY_TRIGGER_INTERVAL_MINUTES = 1;
  private final AtomicReference<Future<?>> retryTriggerFuture = new AtomicReference<>();
  private final IoTDBThriftConnectorV1 retryConnector = new IoTDBThriftConnectorV1();
  private final PriorityQueue<Pair<Long, Event>> retryEventQueue =
      new PriorityQueue<>(Comparator.comparing(o -> o.left));

  private final AtomicLong commitIdGenerator = new AtomicLong(0);
  private final AtomicLong lastCommitId = new AtomicLong(0);
  private final PriorityQueue<Pair<Long, Runnable>> commitQueue =
      new PriorityQueue<>(Comparator.comparing(o -> o.left));

  public IoTDBThriftConnectorV2() {
    if (ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get() == null) {
      synchronized (IoTDBThriftConnectorV2.class) {
        if (ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get() == null) {
          ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.set(
              new IClientManager.Factory<TEndPoint, AsyncPipeDataTransferServiceClient>()
                  .createClientManager(
                      new ClientPoolFactory.AsyncPipeDataTransferServiceClientPoolFactory()));
        }
      }
    }
    asyncPipeDataTransferClientManager = ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get();
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);
    retryConnector.validate(validator);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    retryConnector.customize(parameters, configuration);
  }

  @Override
  // synchronized to avoid close connector when transfer event
  public synchronized void handshake() throws Exception {
    retryConnector.handshake();
  }

  @Override
  public void heartbeat() {
    retryConnector.heartbeat();
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary();

    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftConnectorV2 only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    final long requestCommitId = commitIdGenerator.incrementAndGet();

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
      final PipeTransferInsertNodeReq pipeTransferInsertNodeReq =
          PipeTransferInsertNodeReq.toTPipeTransferReq(
              pipeInsertNodeTabletInsertionEvent.getInsertNode());
      final PipeTransferInsertNodeTabletInsertionEventHandler pipeTransferInsertNodeReqHandler =
          new PipeTransferInsertNodeTabletInsertionEventHandler(
              requestCommitId, pipeInsertNodeTabletInsertionEvent, pipeTransferInsertNodeReq, this);

      transfer(requestCommitId, pipeTransferInsertNodeReqHandler);
    } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) tabletInsertionEvent;
      final PipeTransferTabletReq pipeTransferTabletReq =
          PipeTransferTabletReq.toTPipeTransferReq(
              pipeRawTabletInsertionEvent.convertToTablet(),
              pipeRawTabletInsertionEvent.isAligned());
      final PipeTransferRawTabletInsertionEventHandler pipeTransferTabletReqHandler =
          new PipeTransferRawTabletInsertionEventHandler(
              requestCommitId, pipeRawTabletInsertionEvent, pipeTransferTabletReq, this);

      transfer(requestCommitId, pipeTransferTabletReqHandler);
    }
  }

  private void transfer(
      long requestCommitId,
      PipeTransferInsertNodeTabletInsertionEventHandler pipeTransferInsertNodeReqHandler) {
    final TEndPoint targetNodeUrl = nodeUrls.get((int) (requestCommitId % nodeUrls.size()));

    try {
      final AsyncPipeDataTransferServiceClient client = borrowClient(targetNodeUrl);

      try {
        pipeTransferInsertNodeReqHandler.transfer(client);
      } catch (TException e) {
        LOGGER.warn(
            String.format(
                "Transfer insert node to receiver %s:%s error, retrying...",
                targetNodeUrl.getIp(), targetNodeUrl.getPort()),
            e);
      }
    } catch (Exception ex) {
      pipeTransferInsertNodeReqHandler.onError(ex);
      LOGGER.warn(
          String.format(
              FAILED_TO_BORROW_CLIENT_FORMATTER, targetNodeUrl.getIp(), targetNodeUrl.getPort()),
          ex);
    }
  }

  private void transfer(
      long requestCommitId,
      PipeTransferRawTabletInsertionEventHandler pipeTransferTabletReqHandler) {
    final TEndPoint targetNodeUrl = nodeUrls.get((int) (requestCommitId % nodeUrls.size()));

    try {
      final AsyncPipeDataTransferServiceClient client = borrowClient(targetNodeUrl);

      try {
        pipeTransferTabletReqHandler.transfer(client);
      } catch (TException e) {
        LOGGER.warn(
            String.format(
                "Transfer tablet to receiver %s:%s error, retrying...",
                targetNodeUrl.getIp(), targetNodeUrl.getPort()),
            e);
      }
    } catch (Exception ex) {
      pipeTransferTabletReqHandler.onError(ex);
      LOGGER.warn(
          String.format(
              FAILED_TO_BORROW_CLIENT_FORMATTER, targetNodeUrl.getIp(), targetNodeUrl.getPort()),
          ex);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary();

    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftConnectorV2 only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }

    final long requestCommitId = commitIdGenerator.incrementAndGet();

    final PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
        (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    final PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler =
        new PipeTransferTsFileInsertionEventHandler(
            requestCommitId, pipeTsFileInsertionEvent, this);

    pipeTsFileInsertionEvent.waitForTsFileClose();
    transfer(requestCommitId, pipeTransferTsFileInsertionEventHandler);
  }

  private void transfer(
      long requestCommitId,
      PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler) {
    final TEndPoint targetNodeUrl = nodeUrls.get((int) (requestCommitId % nodeUrls.size()));

    try {
      final AsyncPipeDataTransferServiceClient client = borrowClient(targetNodeUrl);

      try {
        pipeTransferTsFileInsertionEventHandler.transfer(client);
      } catch (TException e) {
        LOGGER.warn(
            String.format(
                "Transfer tsfile to receiver %s:%s error, retrying...",
                targetNodeUrl.getIp(), targetNodeUrl.getPort()),
            e);
      }
    } catch (Exception ex) {
      pipeTransferTsFileInsertionEventHandler.onError(ex);
      LOGGER.warn(
          String.format(
              FAILED_TO_BORROW_CLIENT_FORMATTER, targetNodeUrl.getIp(), targetNodeUrl.getPort()),
          ex);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    transferQueuedEventsIfNecessary();

    LOGGER.warn("IoTDBThriftConnectorV2 does not support transfer generic event: {}.", event);
  }

  private AsyncPipeDataTransferServiceClient borrowClient(TEndPoint targetNodeUrl)
      throws PipeConnectionException {
    try {
      while (true) {
        final AsyncPipeDataTransferServiceClient client =
            asyncPipeDataTransferClientManager.borrowClient(targetNodeUrl);
        if (handshakeIfNecessary(targetNodeUrl, client)) {
          return client;
        }
      }
    } catch (Exception e) {
      throw new PipeConnectionException(
          String.format(
              FAILED_TO_BORROW_CLIENT_FORMATTER, targetNodeUrl.getIp(), targetNodeUrl.getPort()),
          e);
    }
  }

  /**
   * Handshake with the target if necessary.
   *
   * @param client client to handshake
   * @return true if the handshake is already finished, false if the handshake is not finished yet
   *     and finished in this method
   * @throws Exception if an error occurs.
   */
  private boolean handshakeIfNecessary(
      TEndPoint targetNodeUrl, AsyncPipeDataTransferServiceClient client) throws Exception {
    if (client.isHandshakeFinished()) {
      return true;
    }

    final AtomicBoolean isHandshakeFinished = new AtomicBoolean(false);
    final AtomicReference<Exception> exception = new AtomicReference<>();

    client.pipeTransfer(
        PipeTransferHandshakeReq.toTPipeTransferReq(
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision()),
        new AsyncMethodCallback<TPipeTransferResp>() {
          @Override
          public void onComplete(TPipeTransferResp response) {
            if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              LOGGER.warn(
                  "Handshake error with receiver {}:{}, code: {}, message: {}.",
                  targetNodeUrl.getIp(),
                  targetNodeUrl.getPort(),
                  response.getStatus().getCode(),
                  response.getStatus().getMessage());
              exception.set(
                  new PipeConnectionException(
                      String.format(
                          "Handshake error with receiver %s:%s, code: %d, message: %s.",
                          targetNodeUrl.getIp(),
                          targetNodeUrl.getPort(),
                          response.getStatus().getCode(),
                          response.getStatus().getMessage())));
            } else {
              LOGGER.info(
                  "Handshake successfully with receiver {}:{}.",
                  targetNodeUrl.getIp(),
                  targetNodeUrl.getPort());
              client.markHandshakeFinished();
            }

            isHandshakeFinished.set(true);
          }

          @Override
          public void onError(Exception e) {
            LOGGER.warn(
                "Handshake error with receiver {}:{}.",
                targetNodeUrl.getIp(),
                targetNodeUrl.getPort(),
                e);
            exception.set(e);

            isHandshakeFinished.set(true);
          }
        });

    try {
      while (!isHandshakeFinished.get()) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PipeException("Interrupted while waiting for handshake response.", e);
    }

    if (exception.get() != null) {
      throw new PipeConnectionException("Failed to handshake.", exception.get());
    }

    return false;
  }

  /**
   * Transfer queued events which are waiting for retry.
   *
   * @throws Exception if an error occurs. The error will be handled by pipe framework, which will
   *     retry the event and mark the event as failure and stop the pipe if the retry times exceeds
   *     the threshold.
   * @see PipeConnector#transfer(Event) for more details.
   * @see PipeConnector#transfer(TabletInsertionEvent) for more details.
   * @see PipeConnector#transfer(TsFileInsertionEvent) for more details.
   */
  private synchronized void transferQueuedEventsIfNecessary() throws Exception {
    while (!retryEventQueue.isEmpty()) {
      final Pair<Long, Event> queuedEventPair = retryEventQueue.peek();
      final long requestCommitId = queuedEventPair.getLeft();
      final Event event = queuedEventPair.getRight();

      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        retryConnector.transfer((PipeInsertNodeTabletInsertionEvent) event);
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        retryConnector.transfer((PipeRawTabletInsertionEvent) event);
      } else if (event instanceof PipeTsFileInsertionEvent) {
        retryConnector.transfer((PipeTsFileInsertionEvent) event);
      } else {
        LOGGER.warn("IoTDBThriftConnectorV2 does not support transfer generic event: {}.", event);
      }

      if (event instanceof EnrichedEvent) {
        commit(requestCommitId, (EnrichedEvent) event);
      }

      retryEventQueue.poll();
    }
  }

  /**
   * Commit the event. Decrease the reference count of the event. If the reference count is 0, the
   * progress index of the event will be recalculated and the resources of the event will be
   * released.
   *
   * <p>The synchronization is necessary because the commit order must be the same as the order of
   * the events. Concurrent commit may cause the commit order to be inconsistent with the order of
   * the events.
   *
   * @param requestCommitId commit id of the request
   * @param enrichedEvent event to commit
   */
  public synchronized void commit(long requestCommitId, @Nullable EnrichedEvent enrichedEvent) {
    commitQueue.offer(
        new Pair<>(
            requestCommitId,
            () ->
                Optional.ofNullable(enrichedEvent)
                    .ifPresent(
                        event ->
                            event.decreaseReferenceCount(IoTDBThriftConnectorV2.class.getName()))));

    while (!commitQueue.isEmpty()) {
      final Pair<Long, Runnable> committer = commitQueue.peek();
      if (lastCommitId.get() + 1 != committer.left) {
        break;
      }

      committer.right.run();
      lastCommitId.incrementAndGet();

      commitQueue.poll();
    }
  }

  /**
   * Add failure event to retry queue.
   *
   * @param requestCommitId commit id of the request
   * @param event event to retry
   */
  public void addFailureEventToRetryQueue(long requestCommitId, Event event) {
    if (RETRY_TRIGGER.get() == null) {
      synchronized (IoTDBThriftConnectorV2.class) {
        if (RETRY_TRIGGER.get() == null) {
          RETRY_TRIGGER.set(
              IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
                  ThreadName.PIPE_ASYNC_CONNECTOR_RETRY_TRIGGER.getName()));
        }
      }
    }

    if (retryTriggerFuture.get() == null) {
      synchronized (IoTDBThriftConnectorV2.class) {
        if (retryTriggerFuture.get() == null) {
          retryTriggerFuture.set(
              ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                  RETRY_TRIGGER.get(),
                  () -> {
                    try {
                      transferQueuedEventsIfNecessary();
                    } catch (Exception e) {
                      LOGGER.warn("Failed to trigger retry.", e);
                    }
                  },
                  RETRY_TRIGGER_INTERVAL_MINUTES,
                  RETRY_TRIGGER_INTERVAL_MINUTES,
                  TimeUnit.MINUTES));
        }
      }
    }

    retryEventQueue.offer(new Pair<>(requestCommitId, event));
  }

  @Override
  // synchronized to avoid close connector when transfer event
  public synchronized void close() throws Exception {
    if (retryTriggerFuture.get() != null) {
      retryTriggerFuture.get().cancel(false);
    }

    retryConnector.close();
  }
}
