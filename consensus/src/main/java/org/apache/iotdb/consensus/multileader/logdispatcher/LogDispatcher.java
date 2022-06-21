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
import org.apache.iotdb.consensus.config.MultiLeaderConfig;
import org.apache.iotdb.consensus.multileader.MultiLeaderServerImpl;
import org.apache.iotdb.consensus.multileader.client.AsyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.client.DispatchLogHandler;
import org.apache.iotdb.consensus.multileader.thrift.TLogBatch;
import org.apache.iotdb.consensus.multileader.thrift.TSyncLogReq;
import org.apache.iotdb.consensus.multileader.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.multileader.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.consensus.ratis.Utils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

/** Manage all asynchronous replication threads and corresponding async clients */
public class LogDispatcher {

  private final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);

  private final MultiLeaderServerImpl impl;
  private final List<LogDispatcherThread> threads;
  private final IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager;
  private ExecutorService executorService;

  public LogDispatcher(
      MultiLeaderServerImpl impl,
      IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager) {
    this.impl = impl;
    this.clientManager = clientManager;
    this.threads =
        impl.getConfiguration().stream()
            .filter(x -> !Objects.equals(x, impl.getThisNode()))
            .map(x -> new LogDispatcherThread(x, impl.getConfig()))
            .collect(Collectors.toList());
    if (!threads.isEmpty()) {
      this.executorService =
          IoTDBThreadPoolFactory.newFixedThreadPool(threads.size(), "LogDispatcher");
    }
  }

  public void start() {
    if (!threads.isEmpty()) {
      threads.forEach(executorService::submit);
    }
  }

  public void stop() {
    if (!threads.isEmpty()) {
      threads.forEach(LogDispatcherThread::stop);
      executorService.shutdownNow();
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

    private final MultiLeaderConfig config;
    private final Peer peer;
    private final IndexController controller;
    // A sliding window class that manages asynchronously pendingBatches
    private final SyncStatus syncStatus;
    // A queue used to receive asynchronous replication requests
    private final BlockingQueue<IndexedConsensusRequest> pendingRequest;
    // A container used to cache requests, whose size changes dynamically
    private final List<IndexedConsensusRequest> bufferedRequest = new LinkedList<>();
    // A reader management class that gets requests from the DataRegion
    private final ConsensusReqReader reader =
        (ConsensusReqReader) impl.getStateMachine().read(new GetConsensusReqReaderPlan());
    private volatile boolean stopped = false;

    public LogDispatcherThread(Peer peer, MultiLeaderConfig config) {
      this.peer = peer;
      this.config = config;
      this.pendingRequest =
          new ArrayBlockingQueue<>(config.getReplication().getMaxPendingRequestNumPerNode());
      this.controller =
          new IndexController(
              impl.getStorageDir(), Utils.fromTEndPointToString(peer.getEndpoint()), false);
      this.syncStatus = new SyncStatus(controller, config);
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

    public MultiLeaderConfig getConfig() {
      return config;
    }

    public boolean offer(IndexedConsensusRequest request) {
      return pendingRequest.offer(request);
    }

    public void stop() {
      stopped = true;
    }

    public boolean isStopped() {
      return stopped;
    }

    @Override
    public void run() {
      logger.info("{}: Dispatcher for {} starts", impl.getThisNode(), peer);
      try {
        PendingBatch batch;
        while (!Thread.interrupted() && !stopped) {
          while ((batch = getBatch()).isEmpty()) {
            // we may block here if there is no requests in the queue
            bufferedRequest.add(pendingRequest.take());
            // If write pressure is low, we simply sleep a little to reduce the number of RPC
            if (pendingRequest.size() <= config.getReplication().getMaxRequestPerBatch()) {
              Thread.sleep(config.getReplication().getMaxWaitingTimeForAccumulatingBatchInMs());
            }
          }
          // we may block here if the synchronization pipeline is full
          syncStatus.addNextBatch(batch);
          // sends batch asynchronously and migrates the retry logic into the callback handler
          sendBatchAsync(batch, new DispatchLogHandler(this, batch));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in logDispatcher for peer {}", peer, e);
      }
      logger.info("{}: Dispatcher for {} exits", impl.getThisNode(), peer);
    }

    public PendingBatch getBatch() {
      PendingBatch batch;
      List<TLogBatch> logBatches = new ArrayList<>();
      long startIndex = syncStatus.getNextSendingIndex();
      long maxIndex = impl.getController().getCurrentIndex();
      long endIndex;
      if (bufferedRequest.size() <= config.getReplication().getMaxRequestPerBatch()) {
        // Use drainTo instead of poll to reduce lock overhead
        pendingRequest.drainTo(
            bufferedRequest,
            config.getReplication().getMaxRequestPerBatch() - bufferedRequest.size());
      }
      if (bufferedRequest.isEmpty()) {
        // only execute this after a restart
        endIndex = constructBatchFromWAL(startIndex, maxIndex, logBatches);
        batch = new PendingBatch(startIndex, endIndex, logBatches);
        logger.debug("accumulated a {} from wal", batch);
      } else {
        Iterator<IndexedConsensusRequest> iterator = bufferedRequest.iterator();
        IndexedConsensusRequest prev = iterator.next();
        // Prevents gap between logs. For example, some requests are not written into the queue when
        // the queue is full. In this case, requests need to be loaded from the WAL
        endIndex = constructBatchFromWAL(startIndex, prev.getSearchIndex(), logBatches);
        if (logBatches.size() == config.getReplication().getMaxRequestPerBatch()) {
          batch = new PendingBatch(startIndex, endIndex, logBatches);
          logger.debug("accumulated a {} from wal", batch);
          return batch;
        }
        constructBatchIndexedFromConsensusRequest(prev, logBatches);
        endIndex = prev.getSearchIndex();
        iterator.remove();
        while (iterator.hasNext()
            && logBatches.size() <= config.getReplication().getMaxRequestPerBatch()) {
          IndexedConsensusRequest current = iterator.next();
          // Prevents gap between logs. For example, some logs are not written into the queue when
          // the queue is full. In this case, requests need to be loaded from the WAL
          if (current.getSearchIndex() != prev.getSearchIndex() + 1) {
            endIndex =
                constructBatchFromWAL(prev.getSearchIndex(), current.getSearchIndex(), logBatches);
            if (logBatches.size() == config.getReplication().getMaxRequestPerBatch()) {
              batch = new PendingBatch(startIndex, endIndex, logBatches);
              logger.debug("accumulated a {} from queue and wal", batch);
              return batch;
            }
          }
          constructBatchIndexedFromConsensusRequest(current, logBatches);
          endIndex = current.getSearchIndex();
          prev = current;
          // We might not be able to remove all the elements in the bufferedRequest in the
          // current function, but that's fine, we'll continue processing these elements in the
          // bufferedRequest the next time we go into the function, they're never lost
          iterator.remove();
        }
        batch = new PendingBatch(startIndex, endIndex, logBatches);
        logger.debug("accumulated a {} from queue and wal", batch);
      }
      return batch;
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

    private long constructBatchFromWAL(
        long currentIndex, long maxIndex, List<TLogBatch> logBatches) {
      while (currentIndex < maxIndex
          && logBatches.size() < config.getReplication().getMaxRequestPerBatch()) {
        // TODO iterator
        IConsensusRequest data = reader.getReq(currentIndex++);
        if (data != null) {
          // since WAL can no longer recover FragmentInstance, but only PlanNode, we need to give
          // special flags to use different deserialization methods in the dataRegion stateMachine
          logBatches.add(new TLogBatch(data.serializeToByteBuffer()));
        }
      }
      return currentIndex - 1;
    }

    private void constructBatchIndexedFromConsensusRequest(
        IndexedConsensusRequest request, List<TLogBatch> logBatches) {
      logBatches.add(new TLogBatch(request.serializeToByteBuffer()));
    }
  }
}
