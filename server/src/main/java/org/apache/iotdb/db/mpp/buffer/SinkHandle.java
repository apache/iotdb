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

import org.apache.iotdb.db.mpp.buffer.DataBlockManager.SinkHandleListener;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService;
import org.apache.iotdb.mpp.rpc.thrift.EndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.NewDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class SinkHandle implements ISinkHandle {

  private static final Logger logger = LoggerFactory.getLogger(SinkHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;

  private final String remoteHostname;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final String remoteOperatorId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final DataBlockService.Client client;
  private final TsBlockSerde serde;
  private final SinkHandleListener sinkHandleListener;

  // Use LinkedHashMap to meet 2 needs,
  //   1. Predictable iteration order so that removing buffered tsblocks can be efficient.
  //   2. Fast lookup.
  private final LinkedHashMap<Integer, TsBlock> sequenceIdToTsBlock = new LinkedHashMap<>();

  private volatile ListenableFuture<Void> blocked = immediateFuture(null);
  private int nextSequenceId = 0;
  private int numOfBufferedTsBlocks = 0;
  private long bufferRetainedSizeInBytes;
  private boolean closed;
  private boolean noMoreTsBlocks;
  private Throwable throwable;

  public SinkHandle(
      String remoteHostname,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remoteOperatorId,
      TFragmentInstanceId localFragmentInstanceId,
      LocalMemoryManager localMemoryManager,
      ExecutorService executorService,
      DataBlockService.Client client,
      TsBlockSerde serde,
      SinkHandleListener sinkHandleListener) {
    this.remoteHostname = Validate.notNull(remoteHostname);
    this.remoteFragmentInstanceId = Validate.notNull(remoteFragmentInstanceId);
    this.remoteOperatorId = Validate.notNull(remoteOperatorId);
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.executorService = Validate.notNull(executorService);
    this.client = Validate.notNull(client);
    this.serde = Validate.notNull(serde);
    this.sinkHandleListener = Validate.notNull(sinkHandleListener);
  }

  @Override
  public ListenableFuture<Void> isFull() {
    if (closed) {
      throw new IllegalStateException("Sink handle is closed.");
    }
    return nonCancellationPropagating(blocked);
  }

  private void submitSendNewDataBlockEventTask(int startSequenceId, List<Long> blockSizes) {
    executorService.submit(new SendNewDataBlockEventTask(startSequenceId, blockSizes));
  }

  @Override
  public void send(List<TsBlock> tsBlocks) throws IOException {
    Validate.notNull(tsBlocks, "tsBlocks is null");
    if (throwable != null) {
      throw new IOException(throwable);
    }
    if (closed) {
      throw new IllegalStateException("Sink handle is closed.");
    }
    if (!blocked.isDone()) {
      throw new IllegalStateException("Sink handle is blocked.");
    }
    if (noMoreTsBlocks) {
      return;
    }

    long retainedSizeInBytes = 0L;
    for (TsBlock tsBlock : tsBlocks) {
      retainedSizeInBytes += tsBlock.getRetainedSizeInBytes();
    }
    int startSequenceId;
    List<Long> tsBlockSizes = new ArrayList<>();
    synchronized (this) {
      startSequenceId = nextSequenceId;
      blocked =
          localMemoryManager
              .getQueryPool()
              .reserve(localFragmentInstanceId.getQueryId(), retainedSizeInBytes);
      bufferRetainedSizeInBytes += retainedSizeInBytes;
      for (TsBlock tsBlock : tsBlocks) {
        sequenceIdToTsBlock.put(nextSequenceId, tsBlock);
        nextSequenceId += 1;
      }
      numOfBufferedTsBlocks += tsBlocks.size();
      for (int i = startSequenceId; i < nextSequenceId; i++) {
        tsBlockSizes.add(sequenceIdToTsBlock.get(i).getRetainedSizeInBytes());
      }
    }

    // TODO: consider merge multiple NewDataBlockEvent for less network traffic.
    submitSendNewDataBlockEventTask(startSequenceId, tsBlockSizes);
  }

  @Override
  public void send(int partition, List<TsBlock> tsBlocks) {
    throw new UnsupportedOperationException();
  }

  private void sendEndOfDataBlockEvent() throws TException {
    logger.debug(
        "Send end of data block event to operator {} of {}.",
        remoteOperatorId,
        remoteFragmentInstanceId);
    int attempt = 0;
    EndOfDataBlockEvent endOfDataBlockEvent =
        new EndOfDataBlockEvent(
            remoteFragmentInstanceId,
            remoteOperatorId,
            localFragmentInstanceId,
            nextSequenceId - 1);
    while (attempt < MAX_ATTEMPT_TIMES) {
      attempt += 1;
      try {
        client.onEndOfDataBlockEvent(endOfDataBlockEvent);
        break;
      } catch (TException e) {
        logger.error(
            "Failed to send end of data block event to operator {} of {} due to {}, attempt times: {}",
            remoteOperatorId,
            remoteFragmentInstanceId,
            e.getMessage(),
            attempt);
        if (attempt == MAX_ATTEMPT_TIMES) {
          throw e;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    logger.info("Sink handle {} is being closed.", this);
    if (throwable != null) {
      throw new IOException(throwable);
    }
    if (closed) {
      return;
    }
    synchronized (this) {
      closed = true;
      noMoreTsBlocks = true;
    }
    sinkHandleListener.onClosed(this);
    try {
      sendEndOfDataBlockEvent();
    } catch (TException e) {
      throw new IOException(e);
    }
    logger.info("Sink handle {} is closed.", this);
  }

  @Override
  public void abort() {
    logger.info("Sink handle {} is being aborted.", this);
    synchronized (this) {
      sequenceIdToTsBlock.clear();
      numOfBufferedTsBlocks = 0;
      closed = true;
      localMemoryManager
          .getQueryPool()
          .free(localFragmentInstanceId.getQueryId(), bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0;
    }
    sinkHandleListener.onAborted(this);
    logger.info("Sink handle {} is aborted", this);
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    noMoreTsBlocks = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public boolean isFinished() {
    return throwable == null && noMoreTsBlocks && numOfBufferedTsBlocks == 0;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  @Override
  public int getNumOfBufferedTsBlocks() {
    return numOfBufferedTsBlocks;
  }

  ByteBuffer getSerializedTsBlock(int partition, int sequenceId) {
    throw new UnsupportedOperationException();
  }

  ByteBuffer getSerializedTsBlock(int sequenceId) {
    TsBlock tsBlock;
    tsBlock = sequenceIdToTsBlock.get(sequenceId);
    if (tsBlock == null) {
      throw new IllegalStateException("The data block doesn't exist. Sequence ID: " + sequenceId);
    }
    return serde.serialized(tsBlock);
  }

  void acknowledgeTsBlock(int startSequenceId, int endSequenceId) {
    long freedBytes = 0L;
    synchronized (this) {
      Iterator<Entry<Integer, TsBlock>> iterator = sequenceIdToTsBlock.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<Integer, TsBlock> entry = iterator.next();
        if (entry.getKey() < startSequenceId) {
          continue;
        }
        if (entry.getKey() >= endSequenceId) {
          break;
        }
        freedBytes += entry.getValue().getRetainedSizeInBytes();
        numOfBufferedTsBlocks -= 1;
        bufferRetainedSizeInBytes -= entry.getValue().getRetainedSizeInBytes();
        iterator.remove();
      }
    }
    if (isFinished()) {
      sinkHandleListener.onFinish(this);
    }
    localMemoryManager.getQueryPool().free(localFragmentInstanceId.getQueryId(), freedBytes);
  }

  String getRemoteHostname() {
    return remoteHostname;
  }

  TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  String getRemoteOperatorId() {
    return remoteOperatorId;
  }

  TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SinkHandle.class.getSimpleName() + "[", "]")
        .add("remoteHostname='" + remoteHostname + "'")
        .add("remoteFragmentInstanceId=" + remoteFragmentInstanceId)
        .add("remoteOperatorId='" + remoteOperatorId + "'")
        .add("localFragmentInstanceId=" + localFragmentInstanceId)
        .toString();
  }

  /** Send a {@link NewDataBlockEvent} to downstream fragment instance. */
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
      logger.debug(
          "Send new data block event [{}, {}) to operator {} of {}.",
          startSequenceId,
          startSequenceId + blockSizes.size(),
          remoteOperatorId,
          remoteFragmentInstanceId);
      int attempt = 0;
      NewDataBlockEvent newDataBlockEvent =
          new NewDataBlockEvent(
              remoteFragmentInstanceId,
              remoteOperatorId,
              localFragmentInstanceId,
              startSequenceId,
              blockSizes);
      while (attempt < MAX_ATTEMPT_TIMES) {
        attempt += 1;
        try {
          client.onNewDataBlockEvent(newDataBlockEvent);
          break;
        } catch (TException e) {
          logger.error(
              "Failed to send new data block event to operator {} of {} due to {}, attempt times: {}",
              remoteOperatorId,
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
