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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.createFullIdFrom;
import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SinkHandle implements ISinkHandle {

  private static final Logger logger = LoggerFactory.getLogger(SinkHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;
  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000L;

  private final TEndPoint remoteEndpoint;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final String remotePlanNodeId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final TsBlockSerde serde;
  private final SinkHandleListener sinkHandleListener;
  private final String threadName;
  private long retryIntervalInMs;

  // Use LinkedHashMap to meet 2 needs,
  //   1. Predictable iteration order so that removing buffered tsblocks can be efficient.
  //   2. Fast lookup.
  private final LinkedHashMap<Integer, Pair<TsBlock, Long>> sequenceIdToTsBlock =
      new LinkedHashMap<>();

  // size for current TsBlock to reserve and free
  private long currentTsBlockSize;

  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;

  private volatile ListenableFuture<Void> blocked;
  private int nextSequenceId = 0;
  /** The actual buffered memory in bytes, including the amount of memory being reserved. */
  private long bufferRetainedSizeInBytes;

  private boolean aborted = false;

  private boolean closed = false;

  private boolean noMoreTsBlocks = false;

  public SinkHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      TFragmentInstanceId localFragmentInstanceId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SinkHandleListener sinkHandleListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.remoteEndpoint = Validate.notNull(remoteEndpoint);
    this.remoteFragmentInstanceId = Validate.notNull(remoteFragmentInstanceId);
    this.remotePlanNodeId = Validate.notNull(remotePlanNodeId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.executorService = Validate.notNull(executorService);
    this.serde = Validate.notNull(serde);
    this.sinkHandleListener = Validate.notNull(sinkHandleListener);
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
    this.threadName = createFullIdFrom(localFragmentInstanceId, "SinkHandle");
    this.blocked =
        localMemoryManager
            .getQueryPool()
            .reserve(localFragmentInstanceId.getQueryId(), DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES)
            .left;
    this.bufferRetainedSizeInBytes = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    this.currentTsBlockSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public synchronized ListenableFuture<?> isFull() {
    checkState();
    return nonCancellationPropagating(blocked);
  }

  private void submitSendNewDataBlockEventTask(int startSequenceId, List<Long> blockSizes) {
    executorService.submit(new SendNewDataBlockEventTask(startSequenceId, blockSizes));
  }

  @Override
  public synchronized void send(TsBlock tsBlock) {
    Validate.notNull(tsBlock, "tsBlocks is null");
    checkState();
    if (!blocked.isDone()) {
      throw new IllegalStateException("Sink handle is blocked.");
    }
    if (noMoreTsBlocks) {
      return;
    }
    long retainedSizeInBytes = tsBlock.getRetainedSizeInBytes();
    int startSequenceId;
    startSequenceId = nextSequenceId;
    blocked =
        localMemoryManager
            .getQueryPool()
            .reserve(localFragmentInstanceId.getQueryId(), retainedSizeInBytes)
            .left;
    bufferRetainedSizeInBytes += retainedSizeInBytes;

    sequenceIdToTsBlock.put(nextSequenceId, new Pair<>(tsBlock, currentTsBlockSize));
    nextSequenceId += 1;
    currentTsBlockSize = retainedSizeInBytes;

    // TODO: consider merge multiple NewDataBlockEvent for less network traffic.
    submitSendNewDataBlockEventTask(startSequenceId, ImmutableList.of(retainedSizeInBytes));
  }

  @Override
  public synchronized void send(int partition, List<TsBlock> tsBlocks) {
    throw new UnsupportedOperationException();
  }

  private void sendEndOfDataBlockEvent() throws Exception {
    logger.info("send end of data block event");
    int attempt = 0;
    TEndOfDataBlockEvent endOfDataBlockEvent =
        new TEndOfDataBlockEvent(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            nextSequenceId - 1);
    while (attempt < MAX_ATTEMPT_TIMES) {
      attempt += 1;
      try (SyncDataNodeMPPDataExchangeServiceClient client =
          mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
        client.onEndOfDataBlockEvent(endOfDataBlockEvent);
        break;
      } catch (Throwable e) {
        logger.error("Failed to send end of data block event, attempt times: {}", attempt, e);
        if (attempt == MAX_ATTEMPT_TIMES) {
          throw e;
        }
        Thread.sleep(retryIntervalInMs);
      }
    }
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    logger.info("start to set no-more-tsblocks");
    if (aborted || closed) {
      return;
    }
    try {
      sendEndOfDataBlockEvent();
    } catch (Exception e) {
      throw new RuntimeException("Send EndOfDataBlockEvent failed", e);
    }
    logger.info("set noMoreTsBlocks to true");
    noMoreTsBlocks = true;

    if (isFinished()) {
      logger.info("revoke onFinish() of sinkHandleListener");
      sinkHandleListener.onFinish(this);
    }
    logger.info("revoke onEndOfBlocks() of sinkHandleListener");
    sinkHandleListener.onEndOfBlocks(this);
  }

  @Override
  public synchronized void abort() {
    logger.info("SinkHandle is being aborted.");
    sequenceIdToTsBlock.clear();
    aborted = true;
    bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blocked);
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    sinkHandleListener.onAborted(this);
    logger.info("SinkHandle is aborted");
  }

  @Override
  public void close() {
    logger.info("SinkHandle is being closed.");
    sequenceIdToTsBlock.clear();
    closed = true;
    bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryComplete(blocked);
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    sinkHandleListener.onFinish(this);
    logger.info("SinkHandle is closed");
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public boolean isFinished() {
    return noMoreTsBlocks && sequenceIdToTsBlock.isEmpty();
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  public int getNumOfBufferedTsBlocks() {
    return sequenceIdToTsBlock.size();
  }

  ByteBuffer getSerializedTsBlock(int partition, int sequenceId) {
    throw new UnsupportedOperationException();
  }

  ByteBuffer getSerializedTsBlock(int sequenceId) throws IOException {
    TsBlock tsBlock;
    tsBlock = sequenceIdToTsBlock.get(sequenceId).left;
    if (tsBlock == null) {
      throw new IllegalStateException("The data block doesn't exist. Sequence ID: " + sequenceId);
    }
    return serde.serialize(tsBlock);
  }

  void acknowledgeTsBlock(int startSequenceId, int endSequenceId) {
    long freedBytes = 0L;
    synchronized (this) {
      if (aborted || closed) {
        return;
      }
      Iterator<Entry<Integer, Pair<TsBlock, Long>>> iterator =
          sequenceIdToTsBlock.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<Integer, Pair<TsBlock, Long>> entry = iterator.next();
        if (entry.getKey() < startSequenceId) {
          continue;
        }
        if (entry.getKey() >= endSequenceId) {
          break;
        }

        freedBytes += entry.getValue().right;
        bufferRetainedSizeInBytes -= entry.getValue().right;
        iterator.remove();
      }
    }
    if (isFinished()) {
      sinkHandleListener.onFinish(this);
    }
    localMemoryManager.getQueryPool().free(localFragmentInstanceId.getQueryId(), freedBytes);
  }

  public TEndPoint getRemoteEndpoint() {
    return remoteEndpoint;
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  public String getRemotePlanNodeId() {
    return remotePlanNodeId;
  }

  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public String toString() {
    return String.format(
        "Query[%s]-[%s-%s-SinkHandle]:",
        localFragmentInstanceId.queryId,
        localFragmentInstanceId.fragmentId,
        localFragmentInstanceId.instanceId);
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Sink handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("SinkHandle is closed.");
    }
  }

  @TestOnly
  public void setRetryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
  }

  /**
   * Send a {@link org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent} to downstream fragment
   * instance.
   */
  class SendNewDataBlockEventTask implements Runnable {

    private final int startSequenceId;
    private final List<Long> blockSizes;

    SendNewDataBlockEventTask(int startSequenceId, List<Long> blockSizes) {
      Validate.isTrue(
          startSequenceId >= 0,
          "Start sequence ID should be greater than or equal to zero, but was: "
              + startSequenceId
              + ".");
      this.startSequenceId = startSequenceId;
      this.blockSizes = Validate.notNull(blockSizes);
    }

    @Override
    public void run() {
      try (SetThreadName sinkHandleName = new SetThreadName(threadName)) {
        logger.info(
            "Send new data block event [{}, {})",
            startSequenceId,
            startSequenceId + blockSizes.size());
        int attempt = 0;
        TNewDataBlockEvent newDataBlockEvent =
            new TNewDataBlockEvent(
                remoteFragmentInstanceId,
                remotePlanNodeId,
                localFragmentInstanceId,
                startSequenceId,
                blockSizes);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onNewDataBlockEvent(newDataBlockEvent);
            break;
          } catch (Throwable e) {
            logger.error("Failed to send new data block event, attempt times: {}", attempt, e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              sinkHandleListener.onFailure(SinkHandle.this, e);
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              sinkHandleListener.onFailure(SinkHandle.this, e);
            }
          }
        }
      }
    }
  }
}
