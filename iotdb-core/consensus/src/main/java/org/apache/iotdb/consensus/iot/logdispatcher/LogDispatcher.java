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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.DispatchLogHandler;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Manage all asynchronous replication threads and corresponding async clients. */
public class LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  private static final long DEFAULT_INITIAL_SYNC_INDEX = 0L;
  private final IoTConsensusServerImpl impl;
  private final List<LogDispatcherThread> threads;
  private final int selfPeerId;
  private final IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager;
  private ExecutorService executorService;

  private final ConsensusReqReader reader;
  private boolean stopped = false;

  private final AtomicLong logEntriesFromWAL = new AtomicLong(0);
  private final AtomicLong logEntriesFromQueue = new AtomicLong(0);

  public LogDispatcher(
      IoTConsensusServerImpl impl,
      IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager) {
    this.impl = impl;
    this.reader = (ConsensusReqReader) impl.getStateMachine().read(new GetConsensusReqReaderPlan());
    this.selfPeerId = impl.getThisNode().getNodeId();
    this.clientManager = clientManager;
    this.threads =
        impl.getConfiguration().stream()
            .filter(x -> !Objects.equals(x, impl.getThisNode()))
            .map(x -> new LogDispatcherThread(x, impl.getConfig(), DEFAULT_INITIAL_SYNC_INDEX))
            .collect(Collectors.toList());
    if (!threads.isEmpty()) {
      initLogSyncThreadPool();
    }
  }

  private void initLogSyncThreadPool() {
    // We use cached thread pool here because each LogDispatcherThread will occupy one thread.
    // And every LogDispatcherThread won't release its thread in this pool because it won't stop
    // unless LogDispatcher stop.
    // Thus, the size of this threadPool will be the same as the count of LogDispatcherThread.
    this.executorService =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.LOG_DISPATCHER.getName() + "-" + impl.getThisNode().getGroupId());
  }

  public synchronized void start() {
    if (!threads.isEmpty()) {
      threads.forEach(executorService::submit);
    }
  }

  public synchronized void stop() {
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
    stopped = true;
  }

  public synchronized void addLogDispatcherThread(Peer peer, long initialSyncIndex) {
    if (stopped) {
      return;
    }
    LogDispatcherThread thread = new LogDispatcherThread(peer, impl.getConfig(), initialSyncIndex);
    threads.add(thread);
    // If the initial replica is 1, the executorService won't be initialized. And when adding
    // dispatcher thread, the executorService should be initialized manually
    if (this.executorService == null) {
      initLogSyncThreadPool();
    }
    executorService.submit(thread);
  }

  public synchronized void removeLogDispatcherThread(Peer peer) throws IOException {
    if (stopped) {
      return;
    }
    int threadIndex = -1;
    for (int i = 0; i < threads.size(); i++) {
      if (threads.get(i).peer.equals(peer)) {
        threadIndex = i;
        break;
      }
    }
    if (threadIndex == -1) {
      return;
    }
    threads.get(threadIndex).stop();
    threads.get(threadIndex).cleanup();
    threads.remove(threadIndex);
  }

  public synchronized OptionalLong getMinSyncIndex() {
    return threads.stream().mapToLong(LogDispatcherThread::getCurrentSyncIndex).min();
  }

  public synchronized OptionalLong getMinFlushedSyncIndex() {
    return threads.stream().mapToLong(LogDispatcherThread::getLastFlushedSyncIndex).min();
  }

  public void checkAndFlushIndex() {
    if (!threads.isEmpty()) {
      threads.forEach(
          thread -> {
            IndexController controller = thread.getController();
            controller.update(controller.getCurrentIndex(), true);
          });
      // do not set SafelyDeletedSearchIndex as it is Long.MAX_VALUE when replica is 1
      reader.setSafelyDeletedSearchIndex(impl.getMinFlushedSyncIndex());
    }
  }

  public void offer(IndexedConsensusRequest request) {
    // we don't need to serialize and offer request when replicaNum is 1.
    if (!threads.isEmpty()) {
      request.buildSerializedRequests();
      synchronized (this) {
        threads.forEach(
            thread -> {
              logger.debug(
                  "{}->{}: Push a log to the queue, where the queue length is {}",
                  impl.getThisNode().getGroupId(),
                  thread.getPeer().getEndpoint().getIp(),
                  thread.getPendingEntriesSize());
              if (!thread.offer(request)) {
                logger.debug(
                    "{}: Log queue of {} is full, ignore the log to this node, searchIndex: {}",
                    impl.getThisNode().getGroupId(),
                    thread.getPeer(),
                    request.getSearchIndex());
              }
            });
      }
    }
  }

  public long getLogEntriesFromWAL() {
    return logEntriesFromWAL.get();
  }

  public long getLogEntriesFromQueue() {
    return logEntriesFromQueue.get();
  }

  public class LogDispatcherThread implements Runnable {

    private static final long PENDING_REQUEST_TAKING_TIME_OUT_IN_SEC = 10;
    private static final long START_INDEX = 1;
    private final IoTConsensusConfig config;
    private final Peer peer;
    private final IndexController controller;
    // A sliding window class that manages asynchronous pendingBatches
    private final SyncStatus syncStatus;
    // A queue used to receive asynchronous replication requests
    private final BlockingQueue<IndexedConsensusRequest> pendingEntries;
    // A container used to cache requests, whose size changes dynamically
    private final List<IndexedConsensusRequest> bufferedEntries = new LinkedList<>();
    // A reader management class that gets requests from the DataRegion
    private final ConsensusReqReader reader =
        (ConsensusReqReader) impl.getStateMachine().read(new GetConsensusReqReaderPlan());
    private final IoTConsensusMemoryManager iotConsensusMemoryManager =
        IoTConsensusMemoryManager.getInstance();
    private volatile boolean stopped = false;

    private final ConsensusReqReader.ReqIterator walEntryIterator;

    private final LogDispatcherThreadMetrics logDispatcherThreadMetrics;

    private final CountDownLatch runFinished = new CountDownLatch(1);

    public LogDispatcherThread(Peer peer, IoTConsensusConfig config, long initialSyncIndex) {
      this.peer = peer;
      this.config = config;
      this.pendingEntries = new ArrayBlockingQueue<>(config.getReplication().getMaxQueueLength());
      this.controller =
          new IndexController(
              impl.getStorageDir(),
              peer,
              initialSyncIndex,
              config.getReplication().getCheckpointGap());
      this.syncStatus = new SyncStatus(controller, config);
      this.walEntryIterator = reader.getReqIterator(START_INDEX);
      this.logDispatcherThreadMetrics = new LogDispatcherThreadMetrics(this);
      MetricService.getInstance().addMetricSet(logDispatcherThreadMetrics);
    }

    public IndexController getController() {
      return controller;
    }

    public long getCurrentSyncIndex() {
      return controller.getCurrentIndex();
    }

    public long getLastFlushedSyncIndex() {
      return controller.getLastFlushedIndex();
    }

    public Peer getPeer() {
      return peer;
    }

    public IoTConsensusConfig getConfig() {
      return config;
    }

    public int getPendingEntriesSize() {
      return pendingEntries.size();
    }

    public int getBufferRequestSize() {
      return bufferedEntries.size();
    }

    /** try to offer a request into queue with memory control. */
    public boolean offer(IndexedConsensusRequest indexedConsensusRequest) {
      if (!iotConsensusMemoryManager.reserve(indexedConsensusRequest.getSerializedSize(), true)) {
        return false;
      }
      boolean success;
      try {
        success = pendingEntries.offer(indexedConsensusRequest);
      } catch (Throwable t) {
        // If exception occurs during request offer, the reserved memory should be released
        iotConsensusMemoryManager.free(indexedConsensusRequest.getSerializedSize(), true);
        throw t;
      }
      if (!success) {
        // If offer failed, the reserved memory should be released
        iotConsensusMemoryManager.free(indexedConsensusRequest.getSerializedSize(), true);
      }
      return success;
    }

    /** try to remove a request from queue with memory control. */
    private void releaseReservedMemory(IndexedConsensusRequest indexedConsensusRequest) {
      iotConsensusMemoryManager.free(indexedConsensusRequest.getSerializedSize(), true);
    }

    public void stop() {
      stopped = true;
      try {
        if (!runFinished.await(30, TimeUnit.SECONDS)) {
          logger.info("{}: Dispatcher for {} didn't stop after 30s.", impl.getThisNode(), peer);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      long requestSize = 0;
      for (IndexedConsensusRequest indexedConsensusRequest : pendingEntries) {
        requestSize += indexedConsensusRequest.getSerializedSize();
      }
      pendingEntries.clear();
      iotConsensusMemoryManager.free(requestSize, true);
      requestSize = 0;
      for (IndexedConsensusRequest indexedConsensusRequest : bufferedEntries) {
        requestSize += indexedConsensusRequest.getSerializedSize();
      }
      iotConsensusMemoryManager.free(requestSize, true);
      syncStatus.free();
      MetricService.getInstance().removeMetricSet(logDispatcherThreadMetrics);
    }

    public void cleanup() throws IOException {
      this.controller.cleanupVersionFiles();
    }

    public boolean isStopped() {
      return stopped;
    }

    public IoTConsensusServerImpl getImpl() {
      return impl;
    }

    @Override
    public void run() {
      logger.info("{}: Dispatcher for {} starts", impl.getThisNode(), peer);
      try {
        Batch batch;
        while (!Thread.interrupted() && !stopped) {
          long startTime = System.nanoTime();
          while ((batch = getBatch()).isEmpty()) {
            // we may block here if there is no requests in the queue
            IndexedConsensusRequest request =
                pendingEntries.poll(PENDING_REQUEST_TAKING_TIME_OUT_IN_SEC, TimeUnit.SECONDS);
            if (request != null) {
              bufferedEntries.add(request);
              // If write pressure is low, we simply sleep a little to reduce the number of RPC
              if (pendingEntries.size() <= config.getReplication().getMaxLogEntriesNumPerBatch()
                  && bufferedEntries.isEmpty()) {
                Thread.sleep(config.getReplication().getMaxWaitingTimeForAccumulatingBatchInMs());
              }
            }
            // Immediately check for interrupts after poll and sleep
            if (Thread.interrupted() || stopped) {
              throw new InterruptedException("Interrupted after polling and sleeping");
            }
          }
          // Immediately check for interrupts after a time-consuming getBatch() operation
          if (Thread.interrupted() || stopped) {
            throw new InterruptedException("Interrupted after getting a batch");
          }
          logDispatcherThreadMetrics.recordConstructBatchTime(System.nanoTime() - startTime);
          // we may block here if the synchronization pipeline is full
          syncStatus.addNextBatch(batch);
          logEntriesFromWAL.addAndGet(batch.getLogEntriesNumFromWAL());
          logEntriesFromQueue.addAndGet(
              batch.getLogEntries().size() - batch.getLogEntriesNumFromWAL());
          // sends batch asynchronously and migrates the retry logic into the callback handler
          sendBatchAsync(batch, new DispatchLogHandler(this, logDispatcherThreadMetrics, batch));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in logDispatcher for peer {}", peer, e);
      }
      runFinished.countDown();
      logger.info("{}: Dispatcher for {} exits", impl.getThisNode(), peer);
    }

    public void updateSafelyDeletedSearchIndex() {
      // update safely deleted search index to delete outdated info,
      // indicating that insert nodes whose search index are before this value can be deleted
      // safely.
      //
      // Use minFlushedSyncIndex here to reserve the WAL which are not flushed and support kill -9.
      reader.setSafelyDeletedSearchIndex(impl.getMinFlushedSyncIndex());
      // notify
      if (impl.unblockWrite()) {
        impl.signal();
      }
    }

    public Batch getBatch() {
      long startIndex = syncStatus.getNextSendingIndex();
      long maxIndex;
      synchronized (impl.getIndexObject()) {
        maxIndex = impl.getSearchIndex() + 1;
        logger.debug(
            "{}: startIndex: {}, maxIndex: {}, pendingEntries size: {}, bufferedEntries size: {}",
            impl.getThisNode().getGroupId(),
            startIndex,
            maxIndex,
            getPendingEntriesSize(),
            bufferedEntries.size());
        // Use drainTo instead of poll to reduce lock overhead
        pendingEntries.drainTo(
            bufferedEntries,
            config.getReplication().getMaxLogEntriesNumPerBatch() - bufferedEntries.size());
      }
      // remove all request that searchIndex < startIndex
      Iterator<IndexedConsensusRequest> iterator = bufferedEntries.iterator();
      while (iterator.hasNext()) {
        IndexedConsensusRequest request = iterator.next();
        if (request.getSearchIndex() < startIndex) {
          iterator.remove();
          releaseReservedMemory(request);
        } else {
          break;
        }
      }

      Batch batches = new Batch(config);
      // This condition will be executed in several scenarios:
      // 1. restart
      // 2. The getBatch() is invoked immediately at the moment the PendingEntries are consumed
      // up. To prevent inconsistency here, we use the synchronized logic when calculate value of
      // `maxIndex`
      if (bufferedEntries.isEmpty()) {
        constructBatchFromWAL(startIndex, maxIndex, batches);
        batches.buildIndex();
        logger.debug(
            "{} : accumulated a {} from wal when empty", impl.getThisNode().getGroupId(), batches);
      } else {
        // Notice that prev searchIndex >= startIndex
        iterator = bufferedEntries.iterator();
        IndexedConsensusRequest prev = iterator.next();

        // Prevents gap between logs. For example, some requests are not written into the queue when
        // the queue is full. In this case, requests need to be loaded from the WAL
        if (startIndex != prev.getSearchIndex()) {
          boolean hasCorruptedData =
              constructBatchFromWAL(startIndex, prev.getSearchIndex(), batches);
          if (hasCorruptedData || !batches.canAccumulate()) {
            batches.buildIndex();
            logger.debug(
                "{} : accumulated a {} from wal", impl.getThisNode().getGroupId(), batches);
            return batches;
          }
        }

        constructBatchIndexedFromConsensusRequest(prev, batches);
        iterator.remove();
        releaseReservedMemory(prev);
        if (!batches.canAccumulate()) {
          batches.buildIndex();
          logger.debug(
              "{} : accumulated a {} from queue", impl.getThisNode().getGroupId(), batches);
          return batches;
        }

        while (iterator.hasNext() && batches.canAccumulate()) {
          IndexedConsensusRequest current = iterator.next();
          // Prevents gap between logs. For example, some logs are not written into the queue when
          // the queue is full. In this case, requests need to be loaded from the WAL
          if (current.getSearchIndex() != prev.getSearchIndex() + 1) {
            boolean hasCorruptedData =
                constructBatchFromWAL(prev.getSearchIndex() + 1, current.getSearchIndex(), batches);
            if (hasCorruptedData || !batches.canAccumulate()) {
              batches.buildIndex();
              logger.debug(
                  "gap {} : accumulated a {} from queue and wal when gap",
                  impl.getThisNode().getGroupId(),
                  batches);
              return batches;
            }
          }
          constructBatchIndexedFromConsensusRequest(current, batches);
          prev = current;
          // We might not be able to remove all the elements in the bufferedEntries in the
          // current function, but that's fine, we'll continue processing these elements in the
          // bufferedEntries the next time we go into the function, they're never lost
          iterator.remove();
          releaseReservedMemory(current);
        }
        batches.buildIndex();
        logger.debug(
            "{} : accumulated a {} from queue and wal", impl.getThisNode().getGroupId(), batches);
      }
      return batches;
    }

    public void sendBatchAsync(Batch batch, DispatchLogHandler handler) {
      try {
        AsyncIoTConsensusServiceClient client = clientManager.borrowClient(peer.getEndpoint());
        TSyncLogEntriesReq req =
            new TSyncLogEntriesReq(
                selfPeerId, peer.getGroupId().convertToTConsensusGroupId(), batch.getLogEntries());
        logger.debug(
            "Send Batch[startIndex:{}, endIndex:{}] to ConsensusGroup:{}",
            batch.getStartIndex(),
            batch.getEndIndex(),
            peer.getGroupId().convertToTConsensusGroupId());
        client.syncLogEntries(req, handler);
      } catch (Exception e) {
        logger.error("Can not sync logs to peer {} because", peer, e);
        handler.onError(e);
      }
    }

    public SyncStatus getSyncStatus() {
      return syncStatus;
    }

    private boolean constructBatchFromWAL(long currentIndex, long maxIndex, Batch logBatches) {
      logger.debug(
          "DataRegion[{}]->{}: currentIndex: {}, maxIndex: {}",
          peer.getGroupId().getId(),
          peer.getEndpoint().getIp(),
          currentIndex,
          maxIndex);
      boolean hasCorruptedData = false;
      // targetIndex is the index of request that we need to find
      long targetIndex = currentIndex;
      // Even if there is no WAL files, these code won't produce error.
      walEntryIterator.skipTo(targetIndex);
      while (targetIndex < maxIndex && logBatches.canAccumulate()) {
        logger.debug("construct from WAL for one Entry, index : {}", targetIndex);
        try {
          walEntryIterator.waitForNextReady();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("wait for next WAL entry is interrupted");
        }
        IndexedConsensusRequest data = walEntryIterator.next();
        if (data.getSearchIndex() < targetIndex) {
          // if the index of request is smaller than currentIndex, then continue
          logger.warn(
              "search for one Entry which index is {}, but find a smaller one, index : {}",
              targetIndex,
              data.getSearchIndex());
          continue;
        } else if (data.getSearchIndex() > targetIndex) {
          logger.warn(
              "search for one Entry which index is {}, but find a larger one, index : {}."
                  + "Perhaps the wal file is corrupted, in which case we skip it and choose a larger index to replicate",
              targetIndex,
              data.getSearchIndex());
          hasCorruptedData = true;
        }
        targetIndex = data.getSearchIndex() + 1;
        data.buildSerializedRequests();
        // construct request from wal
        logBatches.addTLogEntry(
            new TLogEntry(data.getSerializedRequests(), data.getSearchIndex(), true));
      }
      // In the case of corrupt Data, we return true so that we can send a batch as soon as
      // possible, avoiding potential duplication
      return hasCorruptedData;
    }

    private void constructBatchIndexedFromConsensusRequest(
        IndexedConsensusRequest request, Batch logBatches) {
      logBatches.addTLogEntry(
          new TLogEntry(request.getSerializedRequests(), request.getSearchIndex(), false));
    }
  }
}
