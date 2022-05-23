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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.multileader.MultiLeaderServerImpl;
import org.apache.iotdb.consensus.multileader.client.AsyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.client.DispatchLogHandler;
import org.apache.iotdb.consensus.multileader.client.MultiLeaderConsensusClientPool.AsyncMultiLeaderServiceClientPoolFactory;
import org.apache.iotdb.consensus.multileader.conf.MultiLeaderConsensusConfig;
import org.apache.iotdb.consensus.multileader.thrift.TLogBatch;
import org.apache.iotdb.consensus.multileader.thrift.TLogType;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogReq;
import org.apache.iotdb.consensus.multileader.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.ratis.Utils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LogDispatcher {

  private final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 10;

  private final MultiLeaderServerImpl impl;
  private final List<LogDispatcherThread> threads;
  private ExecutorService executorService;
  private IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager;

  public LogDispatcher(MultiLeaderServerImpl impl) {
    this.impl = impl;
    this.threads =
        impl.getConfiguration().stream()
            .filter(x -> !Objects.equals(x, impl.getThisNode()))
            .map(LogDispatcherThread::new)
            .collect(Collectors.toList());
    if (!threads.isEmpty()) {
      this.executorService =
          IoTDBThreadPoolFactory.newFixedThreadPool(threads.size(), "LogDispatcher");
      this.clientManager =
          new IClientManager.Factory<TEndPoint, AsyncMultiLeaderServiceClient>()
              .createClientManager(new AsyncMultiLeaderServiceClientPoolFactory());
    }
  }

  public void start() {
    if (!threads.isEmpty()) {
      threads.forEach(executorService::submit);
    }
  }

  public void stop() {
    if (!threads.isEmpty()) {
      executorService.shutdownNow();
      clientManager.close();
      int timeout = 10;
      try {
        if (!executorService.awaitTermination(timeout, TimeUnit.SECONDS)) {
          logger.error("Unable to shutdown LogDispatcher service after {} seconds", timeout);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected Interruption when closing LogDispatcher service ");
      }
    }
  }

  public OptionalLong getMinSyncIndex() {
    return threads.stream().mapToLong(LogDispatcherThread::getCurrentSyncIndex).min();
  }

  public void offer(IndexedConsensusRequest request) {
    threads.forEach(
        thread -> {
          if (!thread.offer(request)) {
            logger.debug("Log queue of {} is full, ignore the log to this node", thread.getPeer());
          }
        });
  }

  public class LogDispatcherThread implements Runnable {

    private final Peer peer;
    private final IndexController controller;
    private final SyncStatus syncStatus = new SyncStatus(this);
    private final BlockingQueue<IndexedConsensusRequest> pendingRequest =
        new ArrayBlockingQueue<>(MultiLeaderConsensusConfig.MAX_PENDING_REQUEST_NUM_PER_NODE);
    private final List<IndexedConsensusRequest> bufferedRequest = new LinkedList<>();
    private final ConsensusReqReader reader =
        (ConsensusReqReader) impl.getStateMachine().read(null);

    public LogDispatcherThread(Peer peer) {
      this.peer = peer;
      this.controller =
          new IndexController(impl.getStorageDir(), Utils.IPAddress(peer.getEndpoint()), false);
    }

    public IndexController getController() {
      return controller;
    }

    public long getCurrentSyncIndex() {
      return controller.getCurrentIndex();
    }

    public Peer getPeer() {
      return peer;
    }

    public boolean offer(IndexedConsensusRequest request) {
      return pendingRequest.offer(request);
    }

    @Override
    public void run() {
      try {
        PendingBatch batch;
        while (!Thread.interrupted()) {
          while ((batch = getBatch()).isEmpty()) {
            bufferedRequest.add(pendingRequest.take());
          }
          syncStatus.addNextBatch(batch);
          sendBatchAsync(batch, new DispatchLogHandler(this, batch));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption in logDispatcher for peer {}", peer, e);
      } catch (Exception e) {
        logger.error("Unexpected error in logDispatcher for peer {}", peer, e);
      }
    }

    public PendingBatch getBatch() {
      List<TLogBatch> logBatches = new ArrayList<>();
      long startIndex = getNextSendingIndex();
      long maxIndex = impl.getCurrentNodeController().getCurrentIndex();
      long endIndex;
      if (bufferedRequest.size() <= MultiLeaderConsensusConfig.MAX_REQUEST_PER_BATCH) {
        pendingRequest.drainTo(
            bufferedRequest,
            MultiLeaderConsensusConfig.MAX_REQUEST_PER_BATCH - bufferedRequest.size());
      }
      if (bufferedRequest.isEmpty()) {
        // only execute this after a restart
        endIndex = constructBatchFromWAL(startIndex, maxIndex, logBatches);
      } else {
        Iterator<IndexedConsensusRequest> iterator = bufferedRequest.iterator();
        IndexedConsensusRequest prev = iterator.next();
        constructBatchFromWAL(startIndex, prev.getSearchIndex(), logBatches);
        constructBatchIndexedFromConsensusRequest(prev, logBatches);
        endIndex = prev.getSearchIndex();
        iterator.remove();
        while (iterator.hasNext()
            && logBatches.size() <= MultiLeaderConsensusConfig.MAX_REQUEST_PER_BATCH) {
          IndexedConsensusRequest current = iterator.next();
          if (current.getSearchIndex() != prev.getSearchIndex() + 1) {
            constructBatchFromWAL(prev.getSearchIndex(), current.getSearchIndex(), logBatches);
            constructBatchIndexedFromConsensusRequest(current, logBatches);
          } else {
            constructBatchIndexedFromConsensusRequest(current, logBatches);
          }
          endIndex = current.getSearchIndex();
          prev = current;
          iterator.remove();
        }
      }
      return new PendingBatch(startIndex, endIndex, logBatches);
    }

    public void sendBatchAsync(PendingBatch batch, DispatchLogHandler handler) {
      try {
        AsyncMultiLeaderServiceClient client = clientManager.borrowClient(peer.getEndpoint());
        TSyncLogReq req =
            new TSyncLogReq(peer.getGroupId().convertToTConsensusGroupId(), batch.getBatches());
        client.syncLog(req, handler);
      } catch (IOException | TException e) {
        logger.error("Can not sync logs to peer {} because", peer, e);
      }
    }

    public SyncStatus getSyncStatus() {
      return syncStatus;
    }

    private long getNextSendingIndex() {
      return syncStatus.getMaxPendingIndex().orElseGet(controller::getCurrentIndex) + 1;
    }

    private long constructBatchFromWAL(
        long currentIndex, long maxIndex, List<TLogBatch> logBatches) {
      while (currentIndex < maxIndex
          && logBatches.size() <= MultiLeaderConsensusConfig.MAX_REQUEST_PER_BATCH) {
        // TODO iterator
        IConsensusRequest data = reader.getReq(currentIndex++);
        if (data != null) {
          ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
          data.serializeRequest(buffer);
          buffer.flip();
          logBatches.add(new TLogBatch(TLogType.InsertNode, buffer));
        }
      }
      return currentIndex - 1;
    }

    private void constructBatchIndexedFromConsensusRequest(
        IndexedConsensusRequest request, List<TLogBatch> logBatches) {
      ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
      request.serializeRequest(buffer);
      buffer.flip();
      logBatches.add(new TLogBatch(TLogType.FragmentInstance, buffer));
    }
  }
}
