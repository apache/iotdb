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

package org.apache.iotdb.db.mpp.execution.exchange.source;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.mpp.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TCloseLocalSinkChannelEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager.createFullIdFrom;
import static org.apache.iotdb.db.mpp.metric.DataExchangeCostMetricSet.SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL;
import static org.apache.iotdb.db.mpp.metric.DataExchangeCostMetricSet.SOURCE_HANDLE_GET_TSBLOCK_LOCAL;

public class LocalSourceHandle implements ISourceHandle {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalSourceHandle.class);

  public static final int MAX_ATTEMPT_TIMES = 3;

  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = 1000;

  private ExecutorService executorService;

  private int indexOfUpstreamISinkChannel = 0;

  private TEndPoint remoteEndpoint;

  private TFragmentInstanceId remoteFragmentInstanceId;

  private IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;

  private TFragmentInstanceId localFragmentInstanceId;
  private String localPlanNodeId;
  private final SourceHandleListener sourceHandleListener;
  private final SharedTsBlockQueue queue;
  private boolean aborted = false;

  private boolean closed = false;

  private int currSequenceId;

  private final String threadName;

  private static final TsBlockSerde serde = new TsBlockSerde();
  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  // For pipeline
  public LocalSourceHandle(
      SharedTsBlockQueue queue, SourceHandleListener sourceHandleListener, String threadName) {
    this.queue = Validate.notNull(queue);
    this.queue.setSourceHandle(this);
    this.sourceHandleListener = Validate.notNull(sourceHandleListener);
    this.threadName = threadName;
  }

  // For fragment
  public LocalSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      SharedTsBlockQueue queue,
      SourceHandleListener sourceHandleListener,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager,
      ExecutorService executorService,
      int indexOfUpstreamISinkChannel,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId) {
    this.localFragmentInstanceId = Validate.notNull(localFragmentInstanceId);
    this.localPlanNodeId = Validate.notNull(localPlanNodeId);
    this.queue = Validate.notNull(queue);
    this.queue.setSourceHandle(this);
    this.sourceHandleListener = Validate.notNull(sourceHandleListener);
    this.threadName = createFullIdFrom(localFragmentInstanceId, localPlanNodeId);
    this.mppDataExchangeServiceClientManager = mppDataExchangeServiceClientManager;
    this.executorService = executorService;
    this.indexOfUpstreamISinkChannel = indexOfUpstreamISinkChannel;
    this.remoteEndpoint = remoteEndpoint;
    this.remoteFragmentInstanceId = remoteFragmentInstanceId;
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return localFragmentInstanceId;
  }

  @Override
  public String getLocalPlanNodeId() {
    return localPlanNodeId;
  }

  @Override
  public long getBufferRetainedSizeInBytes() {
    return queue.getBufferRetainedSizeInBytes();
  }

  @Override
  public TsBlock receive() {
    long startTime = System.nanoTime();
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      checkState();

      if (!queue.isBlocked().isDone()) {
        throw new IllegalStateException("Source handle is blocked.");
      }
      TsBlock tsBlock;
      synchronized (queue) {
        tsBlock = queue.remove();
      }
      if (tsBlock != null) {
        LOGGER.debug(
            "[GetTsBlockFromQueue] TsBlock:{} size:{}",
            currSequenceId,
            tsBlock.getRetainedSizeInBytes());
        currSequenceId++;
      }
      checkAndInvokeOnFinished();
      return tsBlock;
    } finally {
      QUERY_METRICS.recordDataExchangeCost(
          SOURCE_HANDLE_GET_TSBLOCK_LOCAL, System.nanoTime() - startTime);
    }
  }

  @Override
  public ByteBuffer getSerializedTsBlock() throws IoTDBException {
    TsBlock tsBlock = receive();
    if (tsBlock != null) {
      long startTime = System.nanoTime();
      try {
        return serde.serialize(tsBlock);
      } catch (Exception e) {
        throw new IoTDBException(e, TSStatusCode.TSBLOCK_SERIALIZE_ERROR.getStatusCode());
      } finally {
        QUERY_METRICS.recordDataExchangeCost(
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL, System.nanoTime() - startTime);
      }
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() {
    synchronized (queue) {
      return queue.hasNoMoreTsBlocks() && queue.isEmpty();
    }
  }

  public void checkAndInvokeOnFinished() {
    synchronized (queue) {
      if (isFinished()) {
        // Putting synchronized here rather than marking in method is to avoid deadlock.
        // There are two locks need to invoke this method. One is lock of SharedTsBlockQueue,
        // the other is lock of LocalSourceHandle.
        synchronized (this) {
          sourceHandleListener.onFinished(this);
        }
      }
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    checkState();
    return nonCancellationPropagating(queue.isBlocked());
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public void abort() {
    if (aborted || closed) {
      return;
    }
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      LOGGER.debug("[StartAbortLocalSourceHandle]");
      synchronized (queue) {
        synchronized (this) {
          if (aborted || closed) {
            return;
          }
          queue.abort();
          aborted = true;
          sourceHandleListener.onAborted(this);
        }
      }
      LOGGER.debug("[EndAbortLocalSourceHandle]");
    }
  }

  @Override
  public void abort(Throwable t) {
    if (aborted || closed) {
      return;
    }
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      LOGGER.debug("[StartAbortLocalSourceHandle]");
      synchronized (queue) {
        synchronized (this) {
          if (aborted || closed) {
            return;
          }
          queue.abort(t);
          aborted = true;
          sourceHandleListener.onAborted(this);
        }
      }
      LOGGER.debug("[EndAbortLocalSourceHandle]");
    }
  }

  @Override
  public void close() {
    if (aborted || closed) {
      return;
    }
    try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
      LOGGER.debug("[StartCloseLocalSourceHandle]");
      synchronized (queue) {
        synchronized (this) {
          if (aborted || closed) {
            return;
          }
          queue.close();
          closed = true;
          executorService.submit(new SendCloseLocalSinkChannelEventTask());
          sourceHandleListener.onFinished(this);
        }
      }
      LOGGER.debug("[EndCloseLocalSourceHandle]");
    }
  }

  private void checkState() {
    if (aborted) {
      throw new IllegalStateException("Source handle is aborted.");
    } else if (closed) {
      throw new IllegalStateException("Source Handle is closed.");
    }
  }

  public SharedTsBlockQueue getSharedTsBlockQueue() {
    return queue;
  }

  @Override
  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    // do nothing, the maxBytesCanReserve of SharedTsBlockQueue should be set by corresponding
    // LocalSinkChannel
  }

  class SendCloseLocalSinkChannelEventTask implements Runnable {

    @Override
    public void run() {
      try (SetThreadName sourceHandleName = new SetThreadName(threadName)) {
        LOGGER.debug(
            "[SendCloseLocalSinkChanelEvent] to [ShuffleSinkHandle: {}, index: {}]).",
            remoteFragmentInstanceId,
            indexOfUpstreamISinkChannel);
        int attempt = 0;
        TCloseLocalSinkChannelEvent closeLocalSinkChannelEvent =
            new TCloseLocalSinkChannelEvent(remoteFragmentInstanceId, indexOfUpstreamISinkChannel);
        while (attempt < MAX_ATTEMPT_TIMES) {
          attempt += 1;
          long startTime = System.nanoTime();
          try (SyncDataNodeMPPDataExchangeServiceClient client =
              mppDataExchangeServiceClientManager.borrowClient(remoteEndpoint)) {
            client.onCloseLocalSinkChannelEvent(closeLocalSinkChannelEvent);
            break;
          } catch (Throwable e) {
            LOGGER.warn(
                "[SendCloseLocalSinkChanelEvent] to [ShuffleSinkHandle: {}, index: {}] failed.).",
                remoteFragmentInstanceId,
                indexOfUpstreamISinkChannel);
            if (attempt == MAX_ATTEMPT_TIMES) {
              synchronized (LocalSourceHandle.this) {
                sourceHandleListener.onFailure(LocalSourceHandle.this, e);
              }
            }
            try {
              Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              synchronized (LocalSourceHandle.this) {
                sourceHandleListener.onFailure(LocalSourceHandle.this, e);
              }
            }
          }
        }
      }
    }
  }
}
