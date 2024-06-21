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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeEventBatch implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventBatch.class);

  private final List<Event> events = new ArrayList<>();
  private final List<Long> requestCommitIds = new ArrayList<>();

  private final List<ByteBuffer> binaryBuffers = new ArrayList<>();
  private final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
  private final List<ByteBuffer> tabletBuffers = new ArrayList<>();

  // limit in delayed time
  private final int maxDelayInMs;
  private long firstEventProcessingTime = Long.MIN_VALUE;

  // limit in buffer size
  private final PipeMemoryBlock allocatedMemoryBlock;
  private long totalBufferSize = 0;

  // Used to rate limit when transferring data
  private final Map<Pair<String, Long>, Long> pipe2BytesAccumulated = new HashMap<>();

  public PipeEventBatch(int maxDelayInMs, long requestMaxBatchSizeInBytes) {
    this.maxDelayInMs = maxDelayInMs;
    this.allocatedMemoryBlock =
        PipeResourceManager.memory()
            .tryAllocate(requestMaxBatchSizeInBytes)
            .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 0))
            .setShrinkCallback(
                (oldMemory, newMemory) ->
                    LOGGER.info(
                        "The batch size limit has shrunk from {} to {}.", oldMemory, newMemory))
            .setExpandMethod(
                oldMemory -> Math.min(Math.max(oldMemory, 1) * 2, requestMaxBatchSizeInBytes))
            .setExpandCallback(
                (oldMemory, newMemory) ->
                    LOGGER.info(
                        "The batch size limit has expanded from {} to {}.", oldMemory, newMemory));

    if (getMaxBatchSizeInBytes() != requestMaxBatchSizeInBytes) {
      LOGGER.info(
          "PipeTransferBatchReqBuilder: the max batch size is adjusted from {} to {} due to the "
              + "memory restriction",
          requestMaxBatchSizeInBytes,
          getMaxBatchSizeInBytes());
    }
  }

  /**
   * Try offer {@link Event} into batch if the given {@link Event} is not duplicated.
   *
   * @param event the given {@link Event}
   * @return {@code true} if the batch can be transferred
   */
  public synchronized boolean onEvent(final TabletInsertionEvent event)
      throws IOException, WALPipeException {
    if (!(event instanceof EnrichedEvent)) {
      return false;
    }

    final long requestCommitId = ((EnrichedEvent) event).getCommitId();

    // The deduplication logic here is to avoid the accumulation of the same event in a batch when
    // retrying.
    if ((events.isEmpty() || !events.get(events.size() - 1).equals(event))) {
      // We increase the reference count for this event to determine if the event may be released.
      if (((EnrichedEvent) event)
          .increaseReferenceCount(PipeTransferBatchReqBuilder.class.getName())) {
        events.add(event);
        requestCommitIds.add(requestCommitId);

        final int bufferSize = buildTabletInsertionBuffer(event);
        totalBufferSize += bufferSize;
        pipe2BytesAccumulated.compute(
            new Pair<>(
                ((EnrichedEvent) event).getPipeName(), ((EnrichedEvent) event).getCreationTime()),
            (pipeName, bytesAccumulated) ->
                bytesAccumulated == null ? bufferSize : bytesAccumulated + bufferSize);

        if (firstEventProcessingTime == Long.MIN_VALUE) {
          firstEventProcessingTime = System.currentTimeMillis();
        }
      } else {
        ((EnrichedEvent) event)
            .decreaseReferenceCount(PipeTransferBatchReqBuilder.class.getName(), false);
      }
    }

    return totalBufferSize >= getMaxBatchSizeInBytes()
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  public synchronized void onSuccess() {
    binaryBuffers.clear();
    insertNodeBuffers.clear();
    tabletBuffers.clear();

    events.clear();
    requestCommitIds.clear();

    firstEventProcessingTime = Long.MIN_VALUE;

    totalBufferSize = 0;
    pipe2BytesAccumulated.clear();
  }

  public PipeTransferTabletBatchReq toTPipeTransferReq() throws IOException {
    return PipeTransferTabletBatchReq.toTPipeTransferReq(
        binaryBuffers, insertNodeBuffers, tabletBuffers);
  }

  private long getMaxBatchSizeInBytes() {
    return allocatedMemoryBlock.getMemoryUsageInBytes();
  }

  public boolean isEmpty() {
    return binaryBuffers.isEmpty() && insertNodeBuffers.isEmpty() && tabletBuffers.isEmpty();
  }

  public List<Event> deepCopyEvents() {
    return new ArrayList<>(events);
  }

  public List<Long> deepCopyRequestCommitIds() {
    return new ArrayList<>(requestCommitIds);
  }

  public Map<Pair<String, Long>, Long> deepCopyPipeName2BytesAccumulated() {
    return new HashMap<>(pipe2BytesAccumulated);
  }

  public Map<Pair<String, Long>, Long> getPipe2BytesAccumulated() {
    return pipe2BytesAccumulated;
  }

  private int buildTabletInsertionBuffer(final TabletInsertionEvent event)
      throws IOException, WALPipeException {
    final ByteBuffer buffer;
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      // Read the bytebuffer from the wal file and transfer it directly without serializing or
      // deserializing if possible
      final InsertNode insertNode =
          pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
      if (Objects.isNull(insertNode)) {
        buffer = pipeInsertNodeTabletInsertionEvent.getByteBuffer();
        binaryBuffers.add(buffer);
      } else {
        buffer = insertNode.serializeToByteBuffer();
        insertNodeBuffers.add(buffer);
      }
    } else {
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
          final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
        pipeRawTabletInsertionEvent.convertToTablet().serialize(outputStream);
        ReadWriteIOUtils.write(pipeRawTabletInsertionEvent.isAligned(), outputStream);
        buffer = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      }
      tabletBuffers.add(buffer);
    }
    return buffer.limit();
  }

  @Override
  public synchronized void close() {
    clearEventsReferenceCount(PipeTransferBatchReqBuilder.class.getName());
    allocatedMemoryBlock.close();
  }

  public void decreaseEventsReferenceCount(final String holderMessage, final boolean shouldReport) {
    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).decreaseReferenceCount(holderMessage, shouldReport);
      }
    }
  }

  public void clearEventsReferenceCount(final String holderMessage) {
    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(holderMessage);
      }
    }
  }
}
