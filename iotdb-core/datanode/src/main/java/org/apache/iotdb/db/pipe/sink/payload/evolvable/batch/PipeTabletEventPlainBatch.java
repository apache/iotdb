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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeTabletEventPlainBatch extends PipeTabletEventBatch {
  private final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
  private final List<ByteBuffer> tabletBuffers = new ArrayList<>();

  // Used to rate limit when transferring data
  private final Map<Pair<String, Long>, Long> pipe2BytesAccumulated = new HashMap<>();

  PipeTabletEventPlainBatch(
      final int maxDelayInMs,
      final long requestMaxBatchSizeInBytes,
      final TriLongConsumer recordMetric) {
    super(maxDelayInMs, requestMaxBatchSizeInBytes, recordMetric);
  }

  @Override
  protected boolean constructBatch(final TabletInsertionEvent event) throws IOException {
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

    insertNodeBuffers.clear();
    tabletBuffers.clear();

    pipe2BytesAccumulated.clear();
  }

  public PipeTransferTabletBatchReq toTPipeTransferReq() throws IOException {
    return PipeTransferTabletBatchReq.toTPipeTransferReq(insertNodeBuffers, tabletBuffers);
  }

  public Map<Pair<String, Long>, Long> deepCopyPipeName2BytesAccumulated() {
    return new HashMap<>(pipe2BytesAccumulated);
  }

  public Map<Pair<String, Long>, Long> getPipe2BytesAccumulated() {
    return pipe2BytesAccumulated;
  }

  private int buildTabletInsertionBuffer(final TabletInsertionEvent event) throws IOException {
    final ByteBuffer buffer;
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      // Read the bytebuffer from the wal file and transfer it directly without serializing or
      // deserializing if possible
      final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
      buffer = insertNode.serializeToByteBuffer();
      insertNodeBuffers.add(buffer);
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
}
