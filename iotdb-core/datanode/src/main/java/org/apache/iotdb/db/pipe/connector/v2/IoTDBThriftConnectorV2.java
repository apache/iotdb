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
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.v1.IoTDBThriftConnectorClient;
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
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_NODE_URLS_KEY;

public class IoTDBThriftConnectorV2 implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftConnectorV2.class);

  private static final String FAILED_TO_BORROW_CLIENT_FORMATTER =
      "Failed to borrow client from client pool for receiver %s:%s.";

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final AtomicReference<
          IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>>
      ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER = new AtomicReference<>();
  private final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>
      asyncPipeDataTransferClientManager;

  private static final AtomicReference<ExecutorService> RETRY_EXECUTOR_HOLDER =
      new AtomicReference<>();
  private final ExecutorService retryExecutor;
  private final BlockingQueue<Pair<Long, Event>> retryQueue =
      new PriorityBlockingQueue<>(16, Comparator.comparing(o -> o.left));

  private final AtomicLong commitIdGenerator = new AtomicLong(0);
  private final AtomicLong lastCommitId = new AtomicLong(0);
  private final PriorityQueue<Pair<Long, Runnable>> commitQueue =
      new PriorityQueue<>(Comparator.comparing(o -> o.left));

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private List<TEndPoint> nodeUrls;

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

    if (RETRY_EXECUTOR_HOLDER.get() == null) {
      synchronized (IoTDBThriftConnectorV2.class) {
        if (RETRY_EXECUTOR_HOLDER.get() == null) {
          RETRY_EXECUTOR_HOLDER.set(
              IoTDBThreadPoolFactory.newSingleThreadExecutor(
                  ThreadName.PIPE_THRIFT_CONNECTOR_V2_RETRY_POOL.getName()));
        }
      }
    }
    retryExecutor = RETRY_EXECUTOR_HOLDER.get();
  }

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

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // node urls string should be like "localhost:6667,localhost:6668"
    validator.validateRequiredAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    nodeUrls =
        SessionUtils.parseSeedNodeUrls(
            Arrays.asList(parameters.getString(CONNECTOR_IOTDB_NODE_URLS_KEY).split(",")));
    if (nodeUrls.isEmpty()) {
      throw new PipeException("Node urls is empty.");
    } else {
      LOGGER.info("Node urls: {}.", nodeUrls);
    }
  }

  @Override
  public void handshake() throws Exception {
    final TEndPoint firstNodeUrl = nodeUrls.get(0);
    try (IoTDBThriftConnectorClient client =
        new IoTDBThriftConnectorClient(
            new ThriftClientProperty.Builder()
                .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                .build(),
            firstNodeUrl.getIp(),
            firstNodeUrl.getPort())) {
      final TPipeTransferResp resp =
          client.pipeTransfer(
              PipeTransferHandshakeReq.toTPipeTransferReq(
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(
            String.format(
                "Handshake error with receiver %s:%s. Response status: %s.",
                firstNodeUrl.getIp(), firstNodeUrl.getPort(), resp.getStatus()));
      } else {
        LOGGER.info(
            "Handshake success with receiver {}:{}.", firstNodeUrl.getIp(), firstNodeUrl.getPort());
      }
    } catch (TException e) {
      throw new PipeConnectionException(
          String.format(
              "Connect to receiver %s:%s error: %s",
              e.getMessage(), firstNodeUrl.getIp(), firstNodeUrl.getPort()),
          e);
    }
  }

  @Override
  public void heartbeat() {
    // do nothing
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftConnectorV2 only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    final long requestCommitId = commitIdGenerator.incrementAndGet();

    if (!retryQueue.isEmpty()) {
      // maybe retryQueue is full and block here
      retryQueue.put(new Pair<>(requestCommitId, tabletInsertionEvent));
      transferQueuedEvents();
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
      final PipeTransferInsertNodeReq pipeTransferInsertNodeReq =
          PipeTransferInsertNodeReq.toTPipeTransferReq(
              pipeInsertNodeTabletInsertionEvent.getInsertNode());
      final PipeTransferInsertNodeTabletInsertionEventHandler pipeTransferInsertNodeReqHandler =
          new PipeTransferInsertNodeTabletInsertionEventHandler(
              requestCommitId,
              pipeInsertNodeTabletInsertionEvent,
              pipeTransferInsertNodeReq,
              this,
              retryExecutor,
              retryQueue);

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
              requestCommitId, pipeTransferTabletReq, this, retryExecutor, retryQueue);

      transfer(requestCommitId, pipeTransferTabletReqHandler);
    }
  }

  public void transfer(
      long requestCommitId,
      PipeTransferInsertNodeTabletInsertionEventHandler pipeTransferInsertNodeReqHandler) {
    final TEndPoint targetNodeUrl = nodeUrls.get((int) (requestCommitId % nodeUrls.size()));

    try {
      final AsyncPipeDataTransferServiceClient client =
          asyncPipeDataTransferClientManager.borrowClient(targetNodeUrl);

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

  public void transfer(
      long requestCommitId,
      PipeTransferRawTabletInsertionEventHandler pipeTransferTabletReqHandler) {
    final TEndPoint targetNodeUrl = nodeUrls.get((int) (requestCommitId % nodeUrls.size()));

    try {
      final AsyncPipeDataTransferServiceClient client =
          asyncPipeDataTransferClientManager.borrowClient(targetNodeUrl);

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
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftConnectorV2 only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }

    final long requestCommitId = commitIdGenerator.incrementAndGet();

    if (!retryQueue.isEmpty()) {
      // maybe retryQueue is full and block here
      retryQueue.put(new Pair<>(requestCommitId, tsFileInsertionEvent));
      transferQueuedEvents();
      return;
    }

    final PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
        (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    final PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler =
        new PipeTransferTsFileInsertionEventHandler(
            requestCommitId, pipeTsFileInsertionEvent, this, retryExecutor, retryQueue);

    pipeTsFileInsertionEvent.waitForTsFileClose();
    transfer(requestCommitId, pipeTransferTsFileInsertionEventHandler);
  }

  public void transfer(
      long requestCommitId,
      PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler) {
    final TEndPoint targetNodeUrl = nodeUrls.get((int) (requestCommitId % nodeUrls.size()));

    try {
      final AsyncPipeDataTransferServiceClient client =
          asyncPipeDataTransferClientManager.borrowClient(targetNodeUrl);

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

  private synchronized void transferQueuedEvents() throws Exception {
    while (!retryQueue.isEmpty()) {
      final Pair<Long, Event> queuedEventPair = retryQueue.poll();
      final long requestCommitId = queuedEventPair.getLeft();
      final Event event = queuedEventPair.getRight();

      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
            (PipeInsertNodeTabletInsertionEvent) event;
        final PipeTransferInsertNodeReq pipeTransferInsertNodeReq =
            PipeTransferInsertNodeReq.toTPipeTransferReq(
                pipeInsertNodeTabletInsertionEvent.getInsertNode());
        final PipeTransferInsertNodeTabletInsertionEventHandler pipeTransferInsertNodeReqHandler =
            new PipeTransferInsertNodeTabletInsertionEventHandler(
                requestCommitId,
                pipeInsertNodeTabletInsertionEvent,
                pipeTransferInsertNodeReq,
                this,
                retryExecutor,
                retryQueue);

        transfer(requestCommitId, pipeTransferInsertNodeReqHandler);
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
            (PipeRawTabletInsertionEvent) event;
        final PipeTransferTabletReq pipeTransferTabletReq =
            PipeTransferTabletReq.toTPipeTransferReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned());
        final PipeTransferRawTabletInsertionEventHandler pipeTransferTabletReqHandler =
            new PipeTransferRawTabletInsertionEventHandler(
                requestCommitId, pipeTransferTabletReq, this, retryExecutor, retryQueue);

        transfer(requestCommitId, pipeTransferTabletReqHandler);
      } else if (event instanceof PipeTsFileInsertionEvent) {
        final PipeTsFileInsertionEvent pipeTsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
        final PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler =
            new PipeTransferTsFileInsertionEventHandler(
                requestCommitId, pipeTsFileInsertionEvent, this, retryExecutor, retryQueue);

        pipeTsFileInsertionEvent.waitForTsFileClose();
        transfer(requestCommitId, pipeTransferTsFileInsertionEventHandler);
      }
    }
  }

  @Override
  public void transfer(Event event) {
    LOGGER.warn("IoTDBThriftConnectorV2 does not support transfer generic event: {}.", event);
  }

  @Override
  public void close() {
    isClosed.set(true);
  }

  public boolean isClosed() {
    return isClosed.get();
  }
}
