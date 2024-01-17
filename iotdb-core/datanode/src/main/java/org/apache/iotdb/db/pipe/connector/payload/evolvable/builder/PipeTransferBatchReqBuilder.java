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

import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_BATCH_SIZE_KEY;

public abstract class PipeTransferBatchReqBuilder implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferBatchReqBuilder.class);

  protected final List<Event> events = new ArrayList<>();
  protected final List<Long> requestCommitIds = new ArrayList<>();

  protected final List<ByteBuffer> binaryBuffers = new ArrayList<>();
  protected final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
  protected final List<ByteBuffer> tabletBuffers = new ArrayList<>();

  // limit in delayed time
  protected final int maxDelayInMs;
  protected long firstEventProcessingTime = Long.MIN_VALUE;

  // limit in buffer size
  protected final PipeMemoryBlock allocatedMemoryBlock;
  protected long totalBufferSize = 0;

  protected PipeTransferBatchReqBuilder(PipeParameters parameters) {
    maxDelayInMs =
        parameters.getIntOrDefault(
                Arrays.asList(CONNECTOR_IOTDB_BATCH_DELAY_KEY, SINK_IOTDB_BATCH_DELAY_KEY),
                CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE)
            * 1000;

    final long requestMaxBatchSizeInBytes =
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE);

    allocatedMemoryBlock =
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
   * Try offer event into cache if the given event is not duplicated.
   *
   * @param event the given event
   * @return true if the batch can be transferred
   */
  public boolean onEvent(TabletInsertionEvent event) throws IOException, WALPipeException {
    if (!(event instanceof EnrichedEvent)) {
      return false;
    }

    final long requestCommitId = ((EnrichedEvent) event).getCommitId();

    // The deduplication logic here is to avoid the accumulation of the same event in a batch when
    // retrying.
    if ((events.isEmpty() || !events.get(events.size() - 1).equals(event))) {
      events.add(event);
      requestCommitIds.add(requestCommitId);
      final int bufferSize = buildTabletInsertionBuffer(event);

      ((EnrichedEvent) event).increaseReferenceCount(PipeTransferBatchReqBuilder.class.getName());

      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }

      totalBufferSize += bufferSize;
    }

    return totalBufferSize >= getMaxBatchSizeInBytes()
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  public void onSuccess() {
    binaryBuffers.clear();
    insertNodeBuffers.clear();
    tabletBuffers.clear();

    events.clear();
    requestCommitIds.clear();

    firstEventProcessingTime = Long.MIN_VALUE;

    totalBufferSize = 0;
  }

  public PipeTransferTabletBatchReq toTPipeTransferReq() throws IOException {
    return PipeTransferTabletBatchReq.toTPipeTransferReq(
        binaryBuffers, insertNodeBuffers, tabletBuffers);
  }

  protected long getMaxBatchSizeInBytes() {
    return allocatedMemoryBlock.getMemoryUsageInBytes();
  }

  public boolean isEmpty() {
    return binaryBuffers.isEmpty() && insertNodeBuffers.isEmpty() && tabletBuffers.isEmpty();
  }

  protected int buildTabletInsertionBuffer(TabletInsertionEvent event)
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
  public void close() {
    allocatedMemoryBlock.close();
  }
}
