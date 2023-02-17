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
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.createFullIdFrom;

public class SourceHandle implements ISourceHandle {

  private static final Logger logger = LoggerFactory.getLogger(SourceHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;
  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000;

  private final TEndPoint remoteEndpoint;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final String localPlanNodeId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final TsBlockSerde serde;
  private final SourceHandleListener sourceHandleListener;

  private final Map<Integer, Long> sequenceIdToDataBlockSize = new HashMap<>();
  private final Map<Integer, ByteBuffer> sequenceIdToTsBlock = new HashMap<>();

  private final String threadName;
  private long retryIntervalInMs;

  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;

  private SettableFuture<Void> blocked = SettableFuture.create();

  private ListenableFuture<Void> blockedOnMemory;

  /** The actual buffered memory in bytes, including the amount of memory being reserved. */
  private long bufferRetainedSizeInBytes = 0L;

  private int currSequenceId = 0;
  private int nextSequenceId = 0;
  private int lastSequenceId = Integer.MAX_VALUE;
  private boolean aborted = false;

  private boolean closed = false;

  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.remoteEndpoint = Validate.notNull(remoteEndpoint);
    this.remoteFragmentInstanceId = Validate.notNull(remoteFragmentInstanceId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localPlanNodeId = Validate.notNull(localPlanNodeId);
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.executorService = Validate.notNull(executorService);
    this.serde = Validate.notNull(serde);
    this.sourceHandleListener = Validate.notNull(sourceHandleListener);
    this.bufferRetainedSizeInBytes = 0L;
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
    this.threadName = createFullIdFrom(localFragmentInstanceId, localPlanNodeId);
  }

  @Override
  public synchronized TsBlock receive() {
    ByteBuffer tsBlock = getSerializedTsBlock();
    if (tsBlock != null) {
      return serde.deserialize(tsBlock);
    } else {
      return null;
    }
  }

  @Override
  public synchronized ByteBuffer getSerializedTsBlock() {
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {

      checkState();

      if (!blocked.isDone()) {
        throw new IllegalStateException("Source handle is blocked.");
      }

      ByteBuffer tsBlock = sequenceIdToTsBlock.remove(currSequenceId);
      if (tsBlock == null) {
        return null;
      }
      long retainedSize = sequenceIdToDataBlockSize.remove(currSequenceId);
      logger.debug("[GetTsBlockFromBuffer] sequenceId:{}, size:{}", currSequenceId, retainedSize);
      currSequenceId += 1;
      bufferRetainedSizeInBytes -= retainedSize;
      localMemoryManager.getQueryPool().free(localFragmentInstanceId.getQueryId(), retainedSize);

      if (sequenceIdToTsBlock.isEmpty() && !isFinished()) {
        logger.debug("[WaitForMoreTsBlock]");
        blocked = SettableFuture.create();
      }
      if (isFinished()) {
        sourceHandleListener.onFinished(this);
      }
      trySubmitGetDataBlocksTask();
      return tsBlock;
    }
  }

  private synchronized void trySubmitGetDataBlocksTask() {
    if (aborted || closed) {
      return;
    }
    if (blockedOnMemory != null && !blockedOnMemory.isDone()) {
      return;
    }

    final int startSequenceId = nextSequenceId;
    int endSequenceId = nextSequenceId;
    long reservedBytes = 0L;
    Pair<ListenableFuture<Void>, Boolean> pair = null;
    long blockedSize = 0L;
    while (sequenceIdToDataBlockSize.containsKey(endSequenceId)) {
      Long bytesToReserve = sequenceIdToDataBlockSize.get(endSequenceId);
      if (bytesToReserve == null) {
        throw new IllegalStateException("Data block size is null.");
      }
      pair =
          localMemoryManager
              .getQueryPool()
              .reserve(localFragmentInstanceId.getQueryId(), bytesToReserve);
      bufferRetainedSizeInBytes += bytesToReserve;
      endSequenceId += 1;
      reservedBytes += bytesToReserve;
      if (!pair.right) {
        blockedSize = bytesToReserve;
        break;
      }
    }

    if (pair == null) {
      // Next data block not generated yet. Do nothing.
      return;
    }
    nextSequenceId = endSequenceId;

    if (!pair.right) {
      endSequenceId--;
      reservedBytes -= blockedSize;
      // The future being not completed indicates,
      //   1. Memory has been reserved for blocks in [startSequenceId, endSequenceId).
      //   2. Memory reservation for block whose sequence ID equals endSequenceId - 1 is blocked.
      //   3. Have not reserve memory for the rest of blocks.
      //
      //  startSequenceId          endSequenceId - 1  endSequenceId
      //         |-------- reserved --------|--- blocked ---|--- not reserved ---|

      // Schedule another call of trySubmitGetDataBlocksTask for the rest of blocks.
      blockedOnMemory = pair.left;
      final int blockedSequenceId = endSequenceId;
      final long blockedRetainedSize = blockedSize;
      blockedOnMemory.addListener(
          () ->
              executorService.submit(
                  new GetDataBlocksTask(
                      blockedSequenceId, blockedSequenceId + 1, blockedRetainedSize)),
          executorService);
    }

    if (endSequenceId > startSequenceId) {
      executorService.submit(new GetDataBlocksTask(startSequenceId, endSequenceId, reservedBytes));
    }
  }

  @Override
  public synchronized ListenableFuture<?> isBlocked() {
    checkState();
    return nonCancellationPropagating(blocked);
  }

  synchronized void setNoMoreTsBlocks(int lastSequenceId) {
    logger.debug("[ReceiveNoMoreTsBlockEvent]");
    this.lastSequenceId = lastSequenceId;
    if (!blocked.isDone() && remoteTsBlockedConsumedUp()) {
      blocked.set(null);
    }
    if (isFinished()) {
      sourceHandleListener.onFinished(this);
    }
  }

  synchronized void updatePendingDataBlockInfo(int startSequenceId, List<Long> dataBlockSizes) {
    logger.debug(
        "[ReceiveNewTsBlockNotification] [{}, {}), each size is: {}",
        startSequenceId,
        startSequenceId + dataBlockSizes.size(),
        dataBlockSizes);
    for (int i = 0; i < dataBlockSizes.size(); i++) {
      sequenceIdToDataBlockSize.put(i + startSequenceId, dataBlockSizes.get(i));
    }
    trySubmitGetDataBlocksTask();
  }

  @Override
  public synchronized void abort() {
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      if (aborted || closed) {
        return;
      }
      if (blocked != null && !blocked.isDone()) {
        blocked.cancel(true);
      }
      if (blockedOnMemory != null) {
        bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
      }
      sequenceIdToDataBlockSize.clear();
      if (bufferRetainedSizeInBytes > 0) {
        localMemoryManager
            .getQueryPool()
            .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
        bufferRetainedSizeInBytes = 0;
      }
      aborted = true;
      sourceHandleListener.onAborted(this);
    }
  }

  @Override
  public void abort(Throwable t) {
    abort();
  }

  @Override
  public synchronized void close() {
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      if (aborted || closed) {
        return;
      }
      if (blocked != null && !blocked.isDone()) {
        blocked.set(null);
      }
      if (blockedOnMemory != null) {
        bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
      }
      sequenceIdToDataBlockSize.clear();
      if (bufferRetainedSizeInBytes > 0) {
        localMemoryManager
            .getQueryPool()
            .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
        bufferRetainedSizeInBytes = 0;
      }
      closed = true;
      currSequenceId = lastSequenceId + 1;
      sourceHandleListener.onFinished(this);
    }
  }

  @Override
  public boolean isFinished() {
    return remoteTsBlockedConsumedUp();
  }

  // Return true indicates two points:
  //   1. Remote SinkHandle has told SourceHandle the total count of TsBlocks by lastSequenceId
  //   2. All the TsBlocks has been consumed up
  private synchronized boolean remoteTsBlockedConsumedUp() {
    return currSequenceId - 1 == lastSequenceId;
  }

  public TEndPoint getRemoteEndpoint() {
    return remoteEndpoint;
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId.deepCopy();
  }

  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  public String getLocalPlanNodeId() {
    return localPlanNodeId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Source handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("SourceHandle is closed.");
    }
  }

  @Override
  public String toString() {
    return String.format(
        "Query[%s]-[%s-%s-SourceHandle-%s]",
        localFragmentInstanceId.getQueryId(),
        localFragmentInstanceId.getFragmentId(),
        localFragmentInstanceId.getInstanceId(),
        localPlanNodeId);
  }

  @TestOnly
  public void setRetryIntervalInMs(long retryIntervalInMs) {
    this.retryIntervalInMs = retryIntervalInMs;
  }

  /** Get data blocks from an upstream fragment instance. */
  class GetDataBlocksTask implements Runnable {
    private final int startSequenceId;
    private final int endSequenceId;
    private final long reservedBytes;

    GetDataBlocksTask(int startSequenceId, int endSequenceId, long reservedBytes) {
      Validate.isTrue(
          startSequenceId >= 0,
          "Start sequence ID should be greater than or equal to zero. Start sequence ID: "
              + startSequenceId);
      this.startSequenceId = startSequenceId;
      Validate.isTrue(
          endSequenceId > startSequenceId,
          "End sequence ID should be greater than the start sequence ID. Start sequence ID: "
              + startSequenceId
              + ", end sequence ID: "
              + endSequenceId);
      this.endSequenceId = endSequenceId;
      Validate.isTrue(reservedBytes > 0L, "Reserved bytes should be greater than zero.");
      this.reservedBytes = reservedBytes;
    }

    @Override
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        logger.debug("[StartPullTsBlocksFromRemote] [{}, {}) ", startSequenceId, endSequenceId);
        TGetDataBlockRequest req =
            new TGetDataBlockRequest(remoteFragmentInstanceId, startSequenceId, endSequenceId);
        int attempt = 0;
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            TGetDataBlockResponse resp = client.getDataBlock(req);
            List<ByteBuffer> tsBlocks = new ArrayList<>(resp.getTsBlocks().size());
            tsBlocks.addAll(resp.getTsBlocks());

            logger.debug("[EndPullTsBlocksFromRemote] Count:{}", tsBlocks.size());
            executorService.submit(
                new SendAcknowledgeDataBlockEventTask(startSequenceId, endSequenceId));
            synchronized (SourceHandle.this) {
              if (aborted || closed) {
                return;
              }
              for (int i = startSequenceId; i < endSequenceId; i++) {
                sequenceIdToTsBlock.put(i, tsBlocks.get(i - startSequenceId));
              }
              logger.debug("[PutTsBlocksIntoBuffer]");
              if (!blocked.isDone()) {
                blocked.set(null);
              }
            }
            break;
          } catch (Throwable e) {

            logger.warn(
                "failed to get data block [{}, {}), attempt times: {}",
                startSequenceId,
                endSequenceId,
                attempt,
                e);

            // reach retry max times
            if (attempt == MAX_ATTEMPT_TIMES) {
              fail(e);
              return;
            }

            // sleep some time before retrying
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              // if interrupted during sleeping, fast fail and don't retry any more
              fail(e);
              return;
            }
          }
        }
      }
    }

    private void fail(Throwable t) {
      synchronized (SourceHandle.this) {
        if (aborted || closed) {
          return;
        }
        bufferRetainedSizeInBytes -= reservedBytes;
        localMemoryManager.getQueryPool().free(localFragmentInstanceId.getQueryId(), reservedBytes);
        sourceHandleListener.onFailure(SourceHandle.this, t);
      }
    }
  }

  class SendAcknowledgeDataBlockEventTask implements Runnable {

    private final int startSequenceId;
    private final int endSequenceId;

    public SendAcknowledgeDataBlockEventTask(int startSequenceId, int endSequenceId) {
      this.startSequenceId = startSequenceId;
      this.endSequenceId = endSequenceId;
    }

    @Override
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        logger.debug("[SendACKTsBlock] [{}, {}).", startSequenceId, endSequenceId);
        int attempt = 0;
        TAcknowledgeDataBlockEvent acknowledgeDataBlockEvent =
            new TAcknowledgeDataBlockEvent(
                remoteFragmentInstanceId, startSequenceId, endSequenceId);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onAcknowledgeDataBlockEvent(acknowledgeDataBlockEvent);
            break;
          } catch (Throwable e) {
            logger.warn(
                "failed to send ack data block event [{}, {}), attempt times: {}",
                startSequenceId,
                endSequenceId,
                attempt,
                e);
            if (attempt == MAX_ATTEMPT_TIMES) {
              synchronized (SourceHandle.this) {
                sourceHandleListener.onFailure(SourceHandle.this, e);
              }
            }
            try {
              Thread.sleep(retryIntervalInMs);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              synchronized (SourceHandle.this) {
                sourceHandleListener.onFailure(SourceHandle.this, e);
              }
            }
          }
        }
      }
    }
  }
}
