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

import org.apache.iotdb.db.mpp.buffer.IDataBlockManager.SinkHandleListener;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

public class SinkHandle implements ISinkHandle {

  private static final Logger logger = LoggerFactory.getLogger(SinkHandle.class);

  private final String remoteHostname;
  private final TFragmentInstanceId remoteFragmentInstanceId;
  private final String remoteOperatorId;
  private final TFragmentInstanceId localFragmentInstanceId;
  private final LocalMemoryManager localMemoryManager;
  private final ExecutorService executorService;
  private final DataBlockService.Client client;
  private final TsBlockSerde serde;
  private final SinkHandleListener sinkHandleListener;

  // TODO: a better data structure to hold tsblocks
  private final List<TsBlock> bufferedTsBlocks = new LinkedList<>();

  private volatile ListenableFuture<Void> blocked = immediateFuture(null);
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
  public void send(List<TsBlock> tsBlocks) {
    Validate.notNull(tsBlocks, "tsBlocks is null");
    if (throwable != null) {
      throw new RuntimeException(throwable);
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
    int currentEndSequenceId;
    List<Long> tsBlockSizes = new ArrayList<>();
    synchronized (this) {
      currentEndSequenceId = bufferedTsBlocks.size();
      blocked =
          localMemoryManager
              .getQueryPool()
              .reserve(localFragmentInstanceId.getQueryId(), retainedSizeInBytes);
      bufferedTsBlocks.addAll(tsBlocks);
      numOfBufferedTsBlocks += tsBlocks.size();
      bufferRetainedSizeInBytes += retainedSizeInBytes;
      for (int i = currentEndSequenceId; i < currentEndSequenceId + tsBlocks.size(); i++) {
        tsBlockSizes.add(bufferedTsBlocks.get(i).getRetainedSizeInBytes());
      }
    }

    submitSendNewDataBlockEventTask(currentEndSequenceId, tsBlockSizes);
  }

  @Override
  public void send(int partition, List<TsBlock> tsBlocks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer getSerializedTsBlock(int partition, int sequenceId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer getSerializedTsBlock(int sequenceId) {
    TsBlock tsBlock;
    synchronized (this) {
      tsBlock = bufferedTsBlocks.get(sequenceId);
      if (tsBlock == null) {
        throw new IllegalStateException("The data block doesn't exist. Sequence ID: " + sequenceId);
      }
      bufferedTsBlocks.set(sequenceId, null);
      numOfBufferedTsBlocks -= 1;
      bufferRetainedSizeInBytes -= tsBlock.getRetainedSizeInBytes();
    }
    localMemoryManager
        .getQueryPool()
        .free(localFragmentInstanceId.getQueryId(), tsBlock.getRetainedSizeInBytes());
    if (isFinished()) {
      sinkHandleListener.onFinish(this);
    }
    return serde.serialized(tsBlock);
  }

  @Override
  public void setNoMoreTsBlocks() {
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

  private void submitSendEndOfDataBlockEventTask() {
    executorService.submit(new SendEndOfDataBlockEventTask());
  }

  @Override
  public synchronized void close() {
    if (throwable != null) {
      throw new RuntimeException(throwable);
    }
    if (closed) {
      return;
    }
    bufferedTsBlocks.clear();
    numOfBufferedTsBlocks = 0;
    bufferRetainedSizeInBytes = 0;
    closed = true;
    noMoreTsBlocks = true;
    submitSendEndOfDataBlockEventTask();
    sinkHandleListener.onClosed(this);
  }

  @Override
  public synchronized void abort() {
    bufferedTsBlocks.clear();
    numOfBufferedTsBlocks = 0;
    closed = true;
    bufferRetainedSizeInBytes = 0;
    submitSendEndOfDataBlockEventTask();
    sinkHandleListener.onAborted(this);
  }

  public String getRemoteHostname() {
    return remoteHostname;
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  public String getRemoteOperatorId() {
    return remoteOperatorId;
  }

  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  public long getBufferRetainedSizeInBytes() {
    return bufferRetainedSizeInBytes;
  }

  public int getNumOfBufferedTsBlocks() {
    return numOfBufferedTsBlocks;
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
          "Send new data block event [{}, {}) from {} to operator {} of {}.",
          startSequenceId,
          startSequenceId + blockSizes.size(),
          localFragmentInstanceId,
          remoteOperatorId,
          remoteFragmentInstanceId);
      try {
        NewDataBlockEvent newDataBlockEvent =
            new NewDataBlockEvent(
                remoteFragmentInstanceId,
                remoteOperatorId,
                localFragmentInstanceId,
                startSequenceId,
                blockSizes);
        client.onNewDataBlockEvent(newDataBlockEvent);
      } catch (TException e) {
        throwable = e;
      }
    }
  }

  /** Send an {@link EndOfDataBlockEvent} to downstream fragment instance. */
  class SendEndOfDataBlockEventTask implements Runnable {
    @Override
    public void run() {
      logger.debug(
          "Send end of data block event from {} to operator {} of {}.",
          localFragmentInstanceId,
          remoteOperatorId,
          remoteFragmentInstanceId);
      try {
        EndOfDataBlockEvent endOfDataBlockEvent =
            new EndOfDataBlockEvent(
                remoteFragmentInstanceId, remoteOperatorId, localFragmentInstanceId);
        client.onEndOfDataBlockEvent(endOfDataBlockEvent);
      } catch (TException e) {
        throwable = e;
      }
    }
  }
}
