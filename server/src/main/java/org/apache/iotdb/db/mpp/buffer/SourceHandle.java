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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.buffer.DataBlockManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.AcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService;
import org.apache.iotdb.mpp.rpc.thrift.GetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.GetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class SourceHandle implements ISourceHandle {

  private static final Logger logger = LoggerFactory.getLogger(SourceHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;

  private final String remoteHostname;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final String localOperatorId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final DataBlockService.Client client;
  private final TsBlockSerde serde;
  private final SourceHandleListener sourceHandleListener;

  private final Map<Integer, TsBlock> sequenceIdToTsBlock = new HashMap<>();
  private final Map<Integer, Long> sequenceIdToDataBlockSize = new HashMap<>();

  private volatile SettableFuture<Void> blocked = SettableFuture.create();
  private long bufferRetainedSizeInBytes;
  private int currSequenceId = 0;
  private int nextSequenceId = 0;
  private int lastSequenceId = Integer.MAX_VALUE;
  private int numActiveGetDataBlocksTask = 0;
  private boolean noMoreTsBlocks;
  private boolean closed;
  private Throwable throwable;

  public SourceHandle(
      String remoteHostname,
      TFragmentInstanceId remoteFragmentInstanceId,
      TFragmentInstanceId localFragmentInstanceId,
      String localOperatorId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      DataBlockService.Client client,
      TsBlockSerde serde,
      SourceHandleListener sourceHandleListener) {
    this.remoteHostname = Validate.notNull(remoteHostname);
    this.remoteFragmentInstanceId = Validate.notNull(remoteFragmentInstanceId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localOperatorId = Validate.notNull(localOperatorId);
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.executorService = Validate.notNull(executorService);
    this.client = Validate.notNull(client);
    this.serde = Validate.notNull(serde);
    this.sourceHandleListener = Validate.notNull(sourceHandleListener);
    bufferRetainedSizeInBytes = 0L;
  }

  @Override
  public TsBlock receive() throws IOException {
    if (throwable != null) {
      throw new IOException(throwable);
    }
    if (closed) {
      throw new IllegalStateException("Source handle is closed.");
    }
    if (!blocked.isDone()) {
      throw new IllegalStateException("Source handle is blocked.");
    }
    TsBlock tsBlock;
    synchronized (this) {
      tsBlock = sequenceIdToTsBlock.remove(currSequenceId);
      currSequenceId += 1;
      bufferRetainedSizeInBytes -= tsBlock.getRetainedSizeInBytes();
      localMemoryManager
          .getQueryPool()
          .free(localFragmentInstanceId.getQueryId(), tsBlock.getRetainedSizeInBytes());

      if (sequenceIdToTsBlock.isEmpty() && !isFinished()) {
        blocked = SettableFuture.create();
      }
    }
    if (isFinished()) {
      sourceHandleListener.onFinished(this);
    }
    trySubmitGetDataBlocksTask();
    return tsBlock;
  }

  private synchronized void trySubmitGetDataBlocksTask() {
    final int startSequenceId = nextSequenceId;
    int endSequenceId = nextSequenceId;
    long reservedBytes = 0L;
    ListenableFuture<?> future = null;
    while (sequenceIdToDataBlockSize.containsKey(endSequenceId)) {
      Long bytesToReserve = sequenceIdToDataBlockSize.get(endSequenceId);
      if (bytesToReserve == null) {
        throw new IllegalStateException("Data block size is null.");
      }
      future =
          localMemoryManager
              .getQueryPool()
              .reserve(localFragmentInstanceId.getQueryId(), bytesToReserve);
      if (future.isDone()) {
        endSequenceId += 1;
        reservedBytes += bytesToReserve;
        bufferRetainedSizeInBytes += bytesToReserve;
      } else {
        break;
      }
    }

    if (future == null) {
      // Next data block not generated yet. Do nothing.
      return;
    }

    if (future.isDone()) {
      nextSequenceId = endSequenceId;
      executorService.submit(new GetDataBlocksTask(startSequenceId, endSequenceId, reservedBytes));
      numActiveGetDataBlocksTask += 1;
    } else {
      nextSequenceId = endSequenceId + 1;
      // The future being not completed indicates,
      //   1. Memory has been reserved for blocks in [startSequenceId, endSequenceId).
      //   2. Memory reservation for block whose sequence ID equals endSequenceId is blocked.
      //   3. Have not reserve memory for the rest of blocks.
      //
      //  startSequenceId             endSequenceId  endSequenceId + 1
      //         |-------- reserved --------|--- blocked ---|--- not reserved ---|

      if (endSequenceId > startSequenceId) {
        // Memory has been reserved. Submit a GetDataBlocksTask for these blocks.
        executorService.submit(
            new GetDataBlocksTask(startSequenceId, endSequenceId, reservedBytes));
        numActiveGetDataBlocksTask += 1;
      }

      // Submit a GetDataBlocksTask when memory is freed.
      final int sequenceIdOfUnReservedDataBlock = endSequenceId;
      final long sizeOfUnReservedDataBlock = sequenceIdToDataBlockSize.get(endSequenceId);
      future.addListener(
          () -> {
            executorService.submit(
                new GetDataBlocksTask(
                    sequenceIdOfUnReservedDataBlock,
                    sequenceIdOfUnReservedDataBlock + 1,
                    sizeOfUnReservedDataBlock));
            numActiveGetDataBlocksTask += 1;
            bufferRetainedSizeInBytes += sizeOfUnReservedDataBlock;
          },
          executorService);

      // Schedule another call of trySubmitGetDataBlocksTask for the rest of blocks.
      future.addListener(SourceHandle.this::trySubmitGetDataBlocksTask, executorService);
    }
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    if (throwable != null) {
      throw new RuntimeException(throwable);
    }
    if (closed) {
      throw new IllegalStateException("Source handle is closed.");
    }
    return nonCancellationPropagating(blocked);
  }

  synchronized void setNoMoreTsBlocks(int lastSequenceId) {
    this.lastSequenceId = lastSequenceId;
    noMoreTsBlocks = true;
  }

  synchronized void updatePendingDataBlockInfo(int startSequenceId, List<Long> dataBlockSizes) {
    for (int i = 0; i < dataBlockSizes.size(); i++) {
      sequenceIdToDataBlockSize.put(i + startSequenceId, dataBlockSizes.get(i));
    }
    trySubmitGetDataBlocksTask();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    sequenceIdToDataBlockSize.clear();
    if (bufferRetainedSizeInBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    closed = true;
    sourceHandleListener.onClosed(this);
  }

  @Override
  public boolean isFinished() {
    return throwable == null
        && noMoreTsBlocks
        && numActiveGetDataBlocksTask == 0
        && nextSequenceId - 1 == lastSequenceId
        && sequenceIdToTsBlock.isEmpty();
  }

  String getRemoteHostname() {
    return remoteHostname;
  }

  TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId.deepCopy();
  }

  TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  String getLocalOperatorId() {
    return localOperatorId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SourceHandle.class.getSimpleName() + "[", "]")
        .add("remoteHostname='" + remoteHostname + "'")
        .add("remoteFragmentInstanceId=" + remoteFragmentInstanceId)
        .add("localFragmentInstanceId=" + localFragmentInstanceId)
        .add("localOperatorId='" + localOperatorId + "'")
        .toString();
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
      logger.debug(
          "Get data blocks [{}, {}) from {} for operator {} of {}.",
          startSequenceId,
          endSequenceId,
          remoteFragmentInstanceId,
          localOperatorId,
          localOperatorId);
      GetDataBlockRequest req =
          new GetDataBlockRequest(remoteFragmentInstanceId, startSequenceId, endSequenceId);
      int attempt = 0;
      while (attempt < MAX_ATTEMPT_TIMES) {
        attempt += 1;
        try {
          GetDataBlockResponse resp = client.getDataBlock(req);
          List<TsBlock> tsBlocks = new ArrayList<>(resp.getTsBlocks().size());
          for (ByteBuffer byteBuffer : resp.getTsBlocks()) {
            TsBlock tsBlock = serde.deserialize(byteBuffer);
            tsBlocks.add(tsBlock);
          }
          synchronized (SourceHandle.this) {
            if (closed) {
              return;
            }
            for (int i = startSequenceId; i < endSequenceId; i++) {
              sequenceIdToTsBlock.put(i, tsBlocks.get(i - startSequenceId));
            }
            if (!blocked.isDone()) {
              blocked.set(null);
            }
          }
          executorService.submit(
              new SendAcknowledgeDataBlockEventTask(startSequenceId, endSequenceId));
          break;
        } catch (TException e) {
          logger.error(
              "Failed to get data block from {} due to {}, attempt times: {}",
              remoteFragmentInstanceId,
              e.getMessage(),
              attempt);
          if (attempt == MAX_ATTEMPT_TIMES) {
            synchronized (SourceHandle.this) {
              throwable = e;
              bufferRetainedSizeInBytes -= reservedBytes;
              localMemoryManager
                  .getQueryPool()
                  .free(localFragmentInstanceId.getQueryId(), reservedBytes);
            }
          }
        } finally {
          numActiveGetDataBlocksTask -= 1;
        }
      }
      // TODO: try to issue another GetDataBlocksTask to make the query run faster.
    }
  }

  class SendAcknowledgeDataBlockEventTask implements Runnable {

    private int startSequenceId;
    private int endSequenceId;

    public SendAcknowledgeDataBlockEventTask(int startSequenceId, int endSequenceId) {
      this.startSequenceId = startSequenceId;
      this.endSequenceId = endSequenceId;
    }

    @Override
    public void run() {
      logger.debug(
          "Send ack data block event [{}, {}) to {}.",
          startSequenceId,
          endSequenceId,
          remoteFragmentInstanceId);
      int attempt = 0;
      AcknowledgeDataBlockEvent acknowledgeDataBlockEvent =
          new AcknowledgeDataBlockEvent(remoteFragmentInstanceId, startSequenceId, endSequenceId);
      while (attempt < MAX_ATTEMPT_TIMES) {
        attempt += 1;
        try {
          client.onAcknowledgeDataBlockEvent(acknowledgeDataBlockEvent);
          break;
        } catch (TException e) {
          logger.error(
              "Failed to send ack data block event [{}, {}) to {} due to {}, attempt times: {}",
              startSequenceId,
              endSequenceId,
              remoteFragmentInstanceId,
              e.getMessage(),
              attempt);
          if (attempt == MAX_ATTEMPT_TIMES) {
            synchronized (this) {
              throwable = e;
            }
          }
        }
      }
    }
  }
}
