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

package org.apache.iotdb.db.queryengine.execution.exchange.source;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.createFullIdFrom;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.GET_DATA_BLOCK_TASK_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SOURCE_HANDLE_GET_TSBLOCK_REMOTE;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.GET_DATA_BLOCK_NUM_CALLER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER;

public class SourceHandle implements ISourceHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;
  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000;

  private final TEndPoint remoteEndpoint;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final TFragmentInstanceId localFragmentInstanceId;

  private final String fullFragmentInstanceId;
  private final String localPlanNodeId;

  private final int indexOfUpstreamSinkHandle;
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

  /** max bytes this SourceHandle can reserve. */
  private long maxBytesCanReserve =
      IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();

  /**
   * this is set to true after calling isBlocked() at least once which indicates that this
   * SourceHandle needs to output data.
   */
  private boolean canGetTsBlockFromRemote = false;

  private final boolean isHighestPriority;

  private static final DataExchangeCostMetricSet DATA_EXCHANGE_COST_METRIC_SET =
      DataExchangeCostMetricSet.getInstance();
  private static final DataExchangeCountMetricSet DATA_EXCHANGE_COUNT_METRIC_SET =
      DataExchangeCountMetricSet.getInstance();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SourceHandle.class)
          + RamUsageEstimator.shallowSizeOfInstance(TFragmentInstanceId.class) * 2;

  @TestOnly
  @SuppressWarnings("squid:S107")
  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this(
        remoteEndpoint,
        remoteFragmentInstanceId,
        localFragmentInstanceId,
        localPlanNodeId,
        indexOfUpstreamSinkHandle,
        localMemoryManager,
        executorService,
        serde,
        sourceHandleListener,
        false,
        mppDataExchangeServiceClientManager);
  }

  @SuppressWarnings("squid:S107")
  public SourceHandle(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener,
      boolean isHighestPriority,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.remoteEndpoint =
        Validate.notNull(
            remoteEndpoint,
            DataNodeQueryMessages.EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_DE2B5885);
    this.remoteFragmentInstanceId =
        Validate.notNull(
            remoteFragmentInstanceId,
            DataNodeQueryMessages.EXCEPTION_REMOTEFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_C2449A29);
    this.localFragmentInstanceId =
        Validate.notNull(
            localFragmentInstanceId,
            DataNodeQueryMessages.EXCEPTION_LOCALFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_37F5917D);
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId);
    this.localPlanNodeId =
        Validate.notNull(
            localPlanNodeId,
            DataNodeQueryMessages.EXCEPTION_LOCALPLANNODEID_CAN_NOT_BE_NULL_DOT_44A34A33);
    this.indexOfUpstreamSinkHandle = indexOfUpstreamSinkHandle;
    this.localMemoryManager =
        Validate.notNull(
            localMemoryManager,
            DataNodeQueryMessages.EXCEPTION_LOCALMEMORYMANAGER_CAN_NOT_BE_NULL_DOT_7A46C6CE);
    this.executorService =
        Validate.notNull(
            executorService,
            DataNodeQueryMessages.EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_BC459BD4);
    this.serde =
        Validate.notNull(serde, DataNodeQueryMessages.EXCEPTION_SERDE_CAN_NOT_BE_NULL_DOT_D46F66E7);
    this.sourceHandleListener =
        Validate.notNull(
            sourceHandleListener,
            DataNodeQueryMessages.EXCEPTION_SOURCEHANDLELISTENER_CAN_NOT_BE_NULL_DOT_01817F52);
    this.isHighestPriority = isHighestPriority;
    this.bufferRetainedSizeInBytes = 0L;
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.retryIntervalInMs = DEFAULT_RETRY_INTERVAL_IN_MS;
    this.threadName = createFullIdFrom(localFragmentInstanceId, localPlanNodeId);
    localMemoryManager
        .getQueryPool()
        .registerPlanNodeIdToQueryMemoryMap(
            localFragmentInstanceId.queryId, fullFragmentInstanceId, localPlanNodeId);
  }

  @Override
  public synchronized TsBlock receive() {
    ByteBuffer tsBlock = getSerializedTsBlock();
    if (tsBlock != null) {
      long startTime = System.nanoTime();
      try {
        return serde.deserialize(tsBlock);
      } finally {
        DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE, System.nanoTime() - startTime);
      }
    } else {
      return null;
    }
  }

  @Override
  public synchronized ByteBuffer getSerializedTsBlock() {
    long startTime = System.nanoTime();
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      checkState();

      if (!blocked.isDone()) {
        throw new IllegalStateException(DataNodeQueryMessages.SOURCE_HANDLE_IS_BLOCKED);
      }

      ByteBuffer tsBlock = sequenceIdToTsBlock.remove(currSequenceId);
      if (tsBlock == null) {
        return null;
      }
      Long retainedSize = sequenceIdToDataBlockSize.remove(currSequenceId);
      if (retainedSize == null) {
        throw new IllegalStateException(DataNodeQueryMessages.RESERVED_DATA_BLOCK_SIZE_IS_NULL);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.GET_TSBLOCK_FROM_BUFFER, currSequenceId, retainedSize);
      }
      currSequenceId += 1;
      if (retainedSize > 0) {
        bufferRetainedSizeInBytes -= retainedSize;
        localMemoryManager
            .getQueryPool()
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                retainedSize);
      }

      if (sequenceIdToTsBlock.isEmpty() && !isFinished()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(DataNodeQueryMessages.WAIT_FOR_MORE_TSBLOCK);
        }
        blocked = SettableFuture.create();
      }
      if (isFinished()) {
        sourceHandleListener.onFinished(this);
      }
      trySubmitGetDataBlocksTask();
      return tsBlock;
    } finally {
      DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
          SOURCE_HANDLE_GET_TSBLOCK_REMOTE, System.nanoTime() - startTime);
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
        throw new IllegalStateException(DataNodeQueryMessages.DATA_BLOCK_SIZE_IS_NULL);
      }
      MemoryReservationResult reserveResult =
          localMemoryManager
              .getQueryPool()
              .reserveWithPriority(
                  localFragmentInstanceId.getQueryId(),
                  fullFragmentInstanceId,
                  localPlanNodeId,
                  bytesToReserve,
                  maxBytesCanReserve,
                  isHighestPriority);
      pair = new Pair<>(reserveResult.getFuture(), reserveResult.isReserveSuccess());
      // actually reserve size is not equals raw size, update the actually reserve size to the map
      if (reserveResult.getReservedBytes() != bytesToReserve) {
        sequenceIdToDataBlockSize.put(endSequenceId, reserveResult.getReservedBytes());
      }
      bufferRetainedSizeInBytes += reserveResult.getReservedBytes();
      endSequenceId += 1;
      reservedBytes += reserveResult.getReservedBytes();
      if (!Boolean.TRUE.equals(pair.right)) {
        blockedSize = bytesToReserve;
        break;
      }
    }

    if (pair == null) {
      // Next data block not generated yet. Do nothing.
      return;
    }
    nextSequenceId = endSequenceId;

    if (!Boolean.TRUE.equals(pair.right)) {
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
    if (!canGetTsBlockFromRemote) {
      canGetTsBlockFromRemote = true;
      // submit get data task once isBlocked is called to ensure that the blocked future will be
      // completed in case that trySubmitGetDataBlocksTask() is not called.
      trySubmitGetDataBlocksTask();
    }
    return nonCancellationPropagating(blocked);
  }

  public synchronized void setNoMoreTsBlocks(int lastSequenceId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(DataNodeQueryMessages.RECEIVE_NO_MORE_TSBLOCK_EVENT);
    }
    this.lastSequenceId = lastSequenceId;
    if (!blocked.isDone() && remoteTsBlockedConsumedUp()) {
      blocked.set(null);
    }
    if (isFinished()) {
      sourceHandleListener.onFinished(this);
    }
  }

  public synchronized void updatePendingDataBlockInfo(
      int startSequenceId, List<Long> dataBlockSizes) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodeQueryMessages.RECEIVENEWTSBLOCKNOTIFICATION_ARG_ARG_EACH_SIZE_IS_ARG,
          startSequenceId,
          startSequenceId + dataBlockSizes.size(),
          dataBlockSizes);
    }
    for (int i = 0; i < dataBlockSizes.size(); i++) {
      sequenceIdToDataBlockSize.put(i + startSequenceId, dataBlockSizes.get(i));
    }
    if (canGetTsBlockFromRemote) {
      trySubmitGetDataBlocksTask();
    }
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
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                bufferRetainedSizeInBytes);
        bufferRetainedSizeInBytes = 0;
      }
      aborted = true;
      sourceHandleListener.onAborted(this);
    }
  }

  @Override
  public synchronized void abort(Throwable t) {
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      if (aborted || closed) {
        return;
      }
      if (blocked != null && !blocked.isDone()) {
        blocked.setException(t);
      }
      if (blockedOnMemory != null) {
        bufferRetainedSizeInBytes -= localMemoryManager.getQueryPool().tryCancel(blockedOnMemory);
      }
      sequenceIdToDataBlockSize.clear();
      if (bufferRetainedSizeInBytes > 0) {
        localMemoryManager
            .getQueryPool()
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                bufferRetainedSizeInBytes);
        bufferRetainedSizeInBytes = 0;
      }
      aborted = true;
      sourceHandleListener.onAborted(this);
    }
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
            .free(
                localFragmentInstanceId.getQueryId(),
                fullFragmentInstanceId,
                localPlanNodeId,
                bufferRetainedSizeInBytes);
        bufferRetainedSizeInBytes = 0;
      }
      closed = true;
      executorService.submit(new SendCloseSinkChannelEventTask());
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
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = Math.min(this.maxBytesCanReserve, maxBytesCanReserve);
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  private void checkState() {
    if (aborted) {
      if (blocked.isDone()) {
        // try throw underlying exception instead of "Source handle is aborted."
        try {
          blocked.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        } catch (ExecutionException e) {
          throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
        }
      }
      throw new IllegalStateException(DataNodeQueryMessages.SOURCE_HANDLE_IS_ABORTED);
    } else if (closed) {
      throw new IllegalStateException(DataNodeQueryMessages.SOURCEHANDLE_IS_CLOSED);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "Query[%s]-[%s-%s-SourceHandle-%s]",
        localFragmentInstanceId.getQueryId(),
        localFragmentInstanceId.getFragmentId(),
        fullFragmentInstanceId,
        localPlanNodeId);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(threadName)
        + RamUsageEstimator.sizeOf(localPlanNodeId)
        + RamUsageEstimator.sizeOf(fullFragmentInstanceId);
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
          DataNodeQueryMessages
                  .EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_DOT_START_SEQUENCE_ID__D3C0AAB7
              + startSequenceId);
      this.startSequenceId = startSequenceId;
      Validate.isTrue(
          endSequenceId > startSequenceId,
          DataNodeQueryMessages
                  .EXCEPTION_END_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_THE_START_SEQUENCE_ID_DOT_START_SEQUENCE__DF1AA2A1
              + startSequenceId
              + DataNodeQueryMessages.EXCEPTION_COMMA_END_SEQUENCE_ID_COLON_DB1AF173
              + endSequenceId);
      this.endSequenceId = endSequenceId;
      Validate.isTrue(
          reservedBytes > 0L,
          DataNodeQueryMessages.EXCEPTION_RESERVED_BYTES_SHOULD_BE_GREATER_THAN_ZERO_DOT_64086BE5);
      this.reservedBytes = reservedBytes;
    }

    @Override
    @SuppressWarnings("squid:S3776")
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.STARTPULLTSBLOCKSFROMREMOTE_ARG_ARG_ARG_ARG,
              remoteFragmentInstanceId,
              indexOfUpstreamSinkHandle,
              startSequenceId,
              endSequenceId);
        }
        TGetDataBlockRequest req =
            new TGetDataBlockRequest(
                remoteFragmentInstanceId,
                startSequenceId,
                endSequenceId,
                indexOfUpstreamSinkHandle);
        int attempt = 0;
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;

          long startTime = System.nanoTime();
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            TGetDataBlockResponse resp = client.getDataBlock(req);
            int tsBlockNum = resp.getTsBlocks().size();
            if (tsBlockNum == 0) {
              if (!closed) {
                // failed to pull TsBlocks
                LOGGER.warn(
                    DataNodeQueryMessages.FAILED_TO_PULL_TSBLOCKS,
                    localFragmentInstanceId,
                    startSequenceId,
                    endSequenceId,
                    remoteFragmentInstanceId,
                    indexOfUpstreamSinkHandle);
              }
              return;
            }
            List<ByteBuffer> tsBlocks = new ArrayList<>(tsBlockNum);
            tsBlocks.addAll(resp.getTsBlocks());

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(DataNodeQueryMessages.END_PULL_TSBLOCKS_FROM_REMOTE, tsBlockNum);
            }
            DATA_EXCHANGE_COUNT_METRIC_SET.recordDataBlockNum(
                GET_DATA_BLOCK_NUM_CALLER, tsBlockNum);
            executorService.submit(
                new SendAcknowledgeDataBlockEventTask(startSequenceId, endSequenceId));
            synchronized (SourceHandle.this) {
              if (aborted || closed) {
                return;
              }
              for (int i = startSequenceId; i < endSequenceId; i++) {
                sequenceIdToTsBlock.put(i, tsBlocks.get(i - startSequenceId));
              }
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(DataNodeQueryMessages.PUT_TSBLOCKS_INTO_BUFFER);
              }
              if (!blocked.isDone()) {
                blocked.set(null);
              }
            }
            break;
          } catch (Throwable e) {

            LOGGER.warn(
                DataNodeQueryMessages.FAILED_TO_GET_DATA_BLOCK,
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
            }
          } finally {
            DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
                GET_DATA_BLOCK_TASK_CALLER, System.nanoTime() - startTime);
          }
        }
      }
    }

    private void fail(Throwable t) {
      synchronized (SourceHandle.this) {
        if (aborted || closed) {
          return;
        }
        if (reservedBytes > 0) {
          bufferRetainedSizeInBytes -= reservedBytes;
          localMemoryManager
              .getQueryPool()
              .free(
                  localFragmentInstanceId.getQueryId(),
                  fullFragmentInstanceId,
                  localPlanNodeId,
                  reservedBytes);
        }
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
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(DataNodeQueryMessages.SEND_ACK_TSBLOCK, startSequenceId, endSequenceId);
        }
        int attempt = 0;
        TAcknowledgeDataBlockEvent acknowledgeDataBlockEvent =
            new TAcknowledgeDataBlockEvent(
                remoteFragmentInstanceId,
                startSequenceId,
                endSequenceId,
                indexOfUpstreamSinkHandle);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          long startTime = System.nanoTime();
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onAcknowledgeDataBlockEvent(acknowledgeDataBlockEvent);
            break;
          } catch (Throwable e) {
            LOGGER.warn(
                DataNodeQueryMessages.FAILED_TO_SEND_ACK_DATA_BLOCK_EVENT,
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
          } finally {
            DATA_EXCHANGE_COST_METRIC_SET.recordDataExchangeCost(
                ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER, System.nanoTime() - startTime);
            DATA_EXCHANGE_COUNT_METRIC_SET.recordDataBlockNum(
                ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER, endSequenceId - startSequenceId);
          }
        }
      }
    }
  }

  class SendCloseSinkChannelEventTask implements Runnable {

    @Override
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.SENDCLOSESINKCHANNELEVENT_TO_SHUFFLESINKHANDLE_ARG_INDEX_ARG,
              remoteFragmentInstanceId,
              indexOfUpstreamSinkHandle);
        }
        int attempt = 0;
        TCloseSinkChannelEvent closeSinkChannelEvent =
            new TCloseSinkChannelEvent(remoteFragmentInstanceId, indexOfUpstreamSinkHandle);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onCloseSinkChannelEvent(closeSinkChannelEvent);
            break;
          } catch (Throwable e) {
            LOGGER.warn(
                DataNodeQueryMessages.SEND_CLOSE_SINK_CHANNEL_EVENT_FAILED,
                remoteFragmentInstanceId,
                indexOfUpstreamSinkHandle);
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
