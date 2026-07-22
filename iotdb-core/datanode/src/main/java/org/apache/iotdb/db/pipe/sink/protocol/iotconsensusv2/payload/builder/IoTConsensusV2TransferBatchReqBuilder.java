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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.builder;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletBatchReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletInsertNodeReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PLAIN_BATCH_DELAY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_SIZE_KEY;

public abstract class IoTConsensusV2TransferBatchReqBuilder implements AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusV2TransferBatchReqBuilder.class);

  protected final List<EnrichedEvent> events = new ArrayList<>();
  protected final List<Long> requestCommitIds = new ArrayList<>();
  protected final List<TIoTConsensusV2TransferReq> batchReqs = new ArrayList<>();
  // limit in delayed time
  protected final int maxDelayInMs;
  protected final TConsensusGroupId consensusGroupId;
  protected final int thisDataNodeId;
  protected long firstEventProcessingTime = Long.MIN_VALUE;

  // limit in buffer size
  protected final long maxBatchSizeInBytes;
  protected final PipeMemoryBlock allocatedMemoryBlock;
  protected long totalBufferSize = 0;

  protected IoTConsensusV2TransferBatchReqBuilder(
      PipeParameters parameters, TConsensusGroupId consensusGroupId, int thisDataNodeId) {
    final Integer requestMaxDelayInMillis =
        parameters.getIntByKeys(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, SINK_IOTDB_BATCH_DELAY_MS_KEY);
    if (Objects.isNull(requestMaxDelayInMillis)) {
      final int requestMaxDelayInSeconds =
          parameters.getIntOrDefault(
              Arrays.asList(
                  CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY, SINK_IOTDB_BATCH_DELAY_SECONDS_KEY),
              CONNECTOR_IOTDB_PLAIN_BATCH_DELAY_DEFAULT_VALUE);
      maxDelayInMs =
          requestMaxDelayInSeconds < 0 ? Integer.MAX_VALUE : requestMaxDelayInSeconds * 1000;
    } else {
      maxDelayInMs = requestMaxDelayInMillis < 0 ? Integer.MAX_VALUE : requestMaxDelayInMillis;
    }

    this.consensusGroupId = consensusGroupId;
    this.thisDataNodeId = thisDataNodeId;

    maxBatchSizeInBytes =
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE);

    allocatedMemoryBlock = PipeDataNodeResourceManager.memory().forceAllocate(0);
  }

  /**
   * Try offer {@link Event} into cache if the given {@link Event} is not duplicated.
   *
   * @param event the given {@link Event}
   * @return {@link true} if the batch can be transferred
   */
  public synchronized boolean onEvent(TabletInsertionEvent event)
      throws IOException, WALPipeException {
    if (!(event instanceof EnrichedEvent)) {
      return false;
    }

    final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
    final long requestCommitId = enrichedEvent.getReplicateIndexForIoTV2();

    // The deduplication logic here is to avoid the accumulation of the same event in a batch when
    // retrying.
    if ((events.isEmpty() || !events.get(events.size() - 1).equals(event))) {
      if (!enrichedEvent.increaseReferenceCount(
          IoTConsensusV2TransferBatchReqBuilder.class.getName())) {
        LOGGER.warn(DataNodePipeMessages.CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE, event);
        return shouldEmit();
      }

      final int previousEventsSize = events.size();
      final int previousRequestCommitIdsSize = requestCommitIds.size();
      final int previousBatchReqsSize = batchReqs.size();
      try {
        events.add(enrichedEvent);
        requestCommitIds.add(requestCommitId);
        final int bufferSize = buildTabletInsertionBuffer(event);
        increaseTotalBufferSizeAndUpdateMemoryBlock(bufferSize);

        if (firstEventProcessingTime == Long.MIN_VALUE) {
          firstEventProcessingTime = System.currentTimeMillis();
        }
      } catch (final Exception e) {
        rollbackTo(previousEventsSize, previousRequestCommitIdsSize, previousBatchReqsSize);
        if (events.isEmpty()) {
          resetMemoryUsage();
        }
        enrichedEvent.decreaseReferenceCount(
            IoTConsensusV2TransferBatchReqBuilder.class.getName(), false);
        throw e;
      }
    }

    return shouldEmit();
  }

  private boolean shouldEmit() {
    return !events.isEmpty()
        && (totalBufferSize >= getMaxBatchSizeInBytes()
            || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs);
  }

  private void increaseTotalBufferSizeAndUpdateMemoryBlock(final long bufferSize) {
    if (bufferSize <= 0) {
      return;
    }

    final long newTotalBufferSize =
        Math.min(totalBufferSize + bufferSize, getMaxBatchSizeInBytes());
    PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlock, newTotalBufferSize);
    totalBufferSize = newTotalBufferSize;
  }

  private void rollbackTo(
      final int previousEventsSize,
      final int previousRequestCommitIdsSize,
      final int previousBatchReqsSize) {
    events.subList(previousEventsSize, events.size()).clear();
    requestCommitIds.subList(previousRequestCommitIdsSize, requestCommitIds.size()).clear();
    batchReqs.subList(previousBatchReqsSize, batchReqs.size()).clear();
  }

  private void resetMemoryUsage() {
    firstEventProcessingTime = Long.MIN_VALUE;
    totalBufferSize = 0;
    PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlock, 0);
  }

  public synchronized void onSuccess() {
    batchReqs.clear();

    events.clear();
    requestCommitIds.clear();

    resetMemoryUsage();
  }

  public IoTConsensusV2TabletBatchReq toTIoTConsensusV2BatchTransferReq() throws IOException {
    return IoTConsensusV2TabletBatchReq.toTIoTConsensusV2BatchTransferReq(batchReqs);
  }

  protected long getMaxBatchSizeInBytes() {
    return maxBatchSizeInBytes;
  }

  public boolean isEmpty() {
    return batchReqs.isEmpty();
  }

  public List<EnrichedEvent> deepCopyEvents() {
    return new ArrayList<>(events);
  }

  protected int buildTabletInsertionBuffer(TabletInsertionEvent event) throws WALPipeException {
    final ByteBuffer buffer;
    final TCommitId commitId;

    // event instanceof PipeInsertNodeTabletInsertionEvent)
    final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
        (PipeInsertNodeTabletInsertionEvent) event;
    commitId =
        new TCommitId(
            pipeInsertNodeTabletInsertionEvent.getReplicateIndexForIoTV2(),
            pipeInsertNodeTabletInsertionEvent.getCommitterKey().getRestartTimes(),
            pipeInsertNodeTabletInsertionEvent.getRebootTimes());

    // Read the bytebuffer from the wal file and transfer it directly without serializing or
    // deserializing if possible
    final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
    // IoTConsensusV2 will transfer binary data to TIoTConsensusV2TransferReq
    final ProgressIndex progressIndex = pipeInsertNodeTabletInsertionEvent.getProgressIndex();
    buffer = insertNode.serializeToByteBuffer();
    batchReqs.add(
        IoTConsensusV2TabletInsertNodeReq.toTIoTConsensusV2TransferReq(
            insertNode, commitId, consensusGroupId, progressIndex, thisDataNodeId));

    return buffer.limit();
  }

  @Override
  public synchronized void close() {
    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(this.getClass().getName());
      }
    }
    batchReqs.clear();
    events.clear();
    requestCommitIds.clear();
    allocatedMemoryBlock.close();
  }
}
