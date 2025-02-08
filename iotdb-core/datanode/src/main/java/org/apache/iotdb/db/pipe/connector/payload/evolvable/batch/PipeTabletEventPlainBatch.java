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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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

public class PipeTabletEventPlainBatch extends PipeTabletEventBatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletEventPlainBatch.class);

  private final List<ByteBuffer> binaryBuffers = new ArrayList<>();
  private final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
  private final List<ByteBuffer> tabletBuffers = new ArrayList<>();

  private static final String TREE_MODEL_DATABASE_PLACEHOLDER = null;
  private final List<String> binaryDataBases = new ArrayList<>();
  private final List<String> insertNodeDataBases = new ArrayList<>();
  private final List<String> tabletDataBases = new ArrayList<>();

  // limit in buffer size
  private final PipeMemoryBlock allocatedMemoryBlock;

  // Used to rate limit when transferring data
  private final Map<Pair<String, Long>, Long> pipe2BytesAccumulated = new HashMap<>();

  PipeTabletEventPlainBatch(final int maxDelayInMs, final long requestMaxBatchSizeInBytes) {
    super(maxDelayInMs);
    this.allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
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
          "PipeTabletEventBatch: the max batch size is adjusted from {} to {} due to the "
              + "memory restriction",
          requestMaxBatchSizeInBytes,
          getMaxBatchSizeInBytes());
    }
  }

  @Override
  protected boolean constructBatch(final TabletInsertionEvent event)
      throws WALPipeException, IOException {
    final int bufferSize = buildTabletInsertionBuffer(event);
    totalBufferSize += bufferSize;
    pipe2BytesAccumulated.compute(
        new Pair<>(
            ((EnrichedEvent) event).getPipeName(), ((EnrichedEvent) event).getCreationTime()),
        (pipeName, bytesAccumulated) ->
            bytesAccumulated == null ? bufferSize : bytesAccumulated + bufferSize);
    return true;
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();

    binaryBuffers.clear();
    insertNodeBuffers.clear();
    tabletBuffers.clear();

    binaryDataBases.clear();
    insertNodeDataBases.clear();
    tabletDataBases.clear();

    pipe2BytesAccumulated.clear();
  }

  public PipeTransferTabletBatchReqV2 toTPipeTransferReq() throws IOException {
    return PipeTransferTabletBatchReqV2.toTPipeTransferReq(
        binaryBuffers,
        insertNodeBuffers,
        tabletBuffers,
        binaryDataBases,
        insertNodeDataBases,
        tabletDataBases);
  }

  @Override
  protected long getMaxBatchSizeInBytes() {
    return allocatedMemoryBlock.getMemoryUsageInBytes();
  }

  public Map<Pair<String, Long>, Long> deepCopyPipeName2BytesAccumulated() {
    return new HashMap<>(pipe2BytesAccumulated);
  }

  public Map<Pair<String, Long>, Long> getPipe2BytesAccumulated() {
    return pipe2BytesAccumulated;
  }

  private int buildTabletInsertionBuffer(final TabletInsertionEvent event)
      throws IOException, WALPipeException {
    int databaseEstimateSize = 0;
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
        if (pipeInsertNodeTabletInsertionEvent.isTableModelEvent()) {
          databaseEstimateSize =
              pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName().length();
          binaryDataBases.add(pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName());
        } else {
          databaseEstimateSize = 4;
          binaryDataBases.add(TREE_MODEL_DATABASE_PLACEHOLDER);
        }
      } else {
        buffer = insertNode.serializeToByteBuffer();
        insertNodeBuffers.add(buffer);
        if (pipeInsertNodeTabletInsertionEvent.isTableModelEvent()) {
          databaseEstimateSize =
              pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName().length();
          insertNodeDataBases.add(pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName());
        } else {
          databaseEstimateSize = 4;
          insertNodeDataBases.add(TREE_MODEL_DATABASE_PLACEHOLDER);
        }
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
      if (pipeRawTabletInsertionEvent.isTableModelEvent()) {
        databaseEstimateSize = pipeRawTabletInsertionEvent.getTableModelDatabaseName().length();
        tabletDataBases.add(pipeRawTabletInsertionEvent.getTableModelDatabaseName());
      } else {
        databaseEstimateSize = 4;
        tabletDataBases.add(TREE_MODEL_DATABASE_PLACEHOLDER);
      }
    }
    return buffer.limit() + databaseEstimateSize;
  }

  @Override
  public synchronized void close() {
    super.close();

    allocatedMemoryBlock.close();
  }
}
