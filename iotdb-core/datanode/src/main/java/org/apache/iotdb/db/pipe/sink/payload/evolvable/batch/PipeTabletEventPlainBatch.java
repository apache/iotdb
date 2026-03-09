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
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTabletEventPlainBatch extends PipeTabletEventBatch {

  private final List<ByteBuffer> binaryBuffers = new ArrayList<>();
  private final List<ByteBuffer> insertNodeBuffers = new ArrayList<>();
  private final List<ByteBuffer> tabletBuffers = new ArrayList<>();

  private static final String TREE_MODEL_DATABASE_PLACEHOLDER = null;
  private final List<String> binaryDataBases = new ArrayList<>();
  private final List<String> insertNodeDataBases = new ArrayList<>();
  private final List<String> tabletDataBases = new ArrayList<>();

  // database -> tableName -> Pair<size, tablets to batch>
  private final Map<String, Map<String, Pair<Integer, List<Tablet>>>> tableModelTabletMap =
      new HashMap<>();

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
    final long bufferSize = buildTabletInsertionBuffer(event);
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
    tableModelTabletMap.clear();

    pipe2BytesAccumulated.clear();
  }

  public PipeTransferTabletBatchReqV2 toTPipeTransferReq() throws IOException {
    for (final Map.Entry<String, Map<String, Pair<Integer, List<Tablet>>>> insertTablets :
        tableModelTabletMap.entrySet()) {
      final String databaseName = insertTablets.getKey();
      for (final Map.Entry<String, Pair<Integer, List<Tablet>>> tabletEntry :
          insertTablets.getValue().entrySet()) {
        // needCopyFlag and tablet
        final List<Pair<Boolean, Tablet>> batchTablets = new ArrayList<>();
        for (final Tablet tablet : tabletEntry.getValue().getRight()) {
          boolean success = false;
          for (final Pair<Boolean, Tablet> tabletPair : batchTablets) {
            if (tabletPair.getLeft()) {
              tabletPair.setRight(copyTablet(tabletPair.getRight()));
              tabletPair.setLeft(Boolean.FALSE);
            }
            if (tabletPair.getRight().append(tablet, tabletEntry.getValue().getLeft())) {
              success = true;
              break;
            }
          }
          if (!success) {
            batchTablets.add(new Pair<>(Boolean.TRUE, tablet));
          }
        }
        for (final Pair<Boolean, Tablet> tabletPair : batchTablets) {
          try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
              final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
            tabletPair.getRight().serialize(outputStream);
            ReadWriteIOUtils.write(true, outputStream);
            tabletBuffers.add(
                ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
          }
          tabletDataBases.add(databaseName);
        }
      }
    }

    tableModelTabletMap.clear();

    return PipeTransferTabletBatchReqV2.toTPipeTransferReq(
        binaryBuffers,
        insertNodeBuffers,
        tabletBuffers,
        binaryDataBases,
        insertNodeDataBases,
        tabletDataBases);
  }

  public Map<Pair<String, Long>, Long> deepCopyPipeName2BytesAccumulated() {
    return new HashMap<>(pipe2BytesAccumulated);
  }

  public Map<Pair<String, Long>, Long> getPipe2BytesAccumulated() {
    return pipe2BytesAccumulated;
  }

  private long buildTabletInsertionBuffer(final TabletInsertionEvent event) throws IOException {
    long estimateSize = 0;
    final ByteBuffer buffer;
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
      if (!(insertNode instanceof RelationalInsertTabletNode)) {
        buffer = insertNode.serializeToByteBuffer();
        insertNodeBuffers.add(buffer);
        if (pipeInsertNodeTabletInsertionEvent.isTableModelEvent()) {
          estimateSize =
              RamUsageEstimator.sizeOf(
                  pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName());
          insertNodeDataBases.add(pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName());
        } else {
          estimateSize = 4;
          insertNodeDataBases.add(TREE_MODEL_DATABASE_PLACEHOLDER);
        }
        estimateSize += buffer.limit();
      } else {
        for (final Tablet tablet :
            ((PipeInsertNodeTabletInsertionEvent) event).convertToTablets()) {
          estimateSize +=
              constructTabletBatch(
                  tablet, pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName());
        }
      }
    } else {
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) event;
      if (pipeRawTabletInsertionEvent.isTableModelEvent()) {
        estimateSize =
            constructTabletBatch(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.getTableModelDatabaseName());
      } else {
        try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
            final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
          pipeRawTabletInsertionEvent.convertToTablet().serialize(outputStream);
          ReadWriteIOUtils.write(pipeRawTabletInsertionEvent.isAligned(), outputStream);
          buffer = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
        }
        estimateSize = 4 + buffer.limit();
        tabletBuffers.add(buffer);
        tabletDataBases.add(TREE_MODEL_DATABASE_PLACEHOLDER);
      }
    }

    return estimateSize;
  }

  private long constructTabletBatch(final Tablet tablet, final String databaseName) {
    final AtomicLong size = new AtomicLong(0);
    final Pair<Integer, List<Tablet>> currentBatch =
        tableModelTabletMap
            .computeIfAbsent(
                databaseName,
                k -> {
                  size.addAndGet(RamUsageEstimator.sizeOf(databaseName));
                  return new HashMap<>();
                })
            .computeIfAbsent(tablet.getTableName(), k -> new Pair<>(0, new ArrayList<>()));
    currentBatch.setLeft(currentBatch.getLeft() + tablet.getRowSize());
    currentBatch.getRight().add(tablet);
    return PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) + 4;
  }

  public static Tablet copyTablet(final Tablet tablet) {
    final Object[] copiedValues = new Object[tablet.getValues().length];
    for (int i = 0; i < tablet.getValues().length; i++) {
      if (tablet.getValues()[i] == null
          || tablet.getSchemas() == null
          || tablet.getSchemas().get(i) == null) {
        continue;
      }
      copiedValues[i] =
          copyValueList(
              tablet.getValues()[i], tablet.getSchemas().get(i).getType(), tablet.getRowSize());
    }

    BitMap[] bitMaps = null;
    if (tablet.getBitMaps() != null) {
      bitMaps =
          Arrays.stream(tablet.getBitMaps())
              .map(
                  bitMap -> {
                    if (bitMap != null) {
                      final byte[] data = bitMap.getByteArray();
                      return new BitMap(bitMap.getSize(), Arrays.copyOf(data, data.length));
                    }
                    return null;
                  })
              .toArray(BitMap[]::new);
    }

    return new Tablet(
        tablet.getTableName(),
        new ArrayList<>(tablet.getSchemas()),
        new ArrayList<>(tablet.getColumnTypes()),
        Arrays.copyOf(tablet.getTimestamps(), tablet.getRowSize()),
        copiedValues,
        bitMaps,
        tablet.getRowSize());
  }

  private static Object copyValueList(
      final Object valueList, final TSDataType dataType, final int rowSize) {
    switch (dataType) {
      case BOOLEAN:
        final boolean[] boolValues = (boolean[]) valueList;
        final boolean[] copiedBoolValues = new boolean[rowSize];
        System.arraycopy(boolValues, 0, copiedBoolValues, 0, rowSize);
        return copiedBoolValues;
      case INT32:
        final int[] intValues = (int[]) valueList;
        final int[] copiedIntValues = new int[rowSize];
        System.arraycopy(intValues, 0, copiedIntValues, 0, rowSize);
        return copiedIntValues;
      case DATE:
        final LocalDate[] dateValues = (LocalDate[]) valueList;
        final LocalDate[] copiedDateValues = new LocalDate[rowSize];
        System.arraycopy(dateValues, 0, copiedDateValues, 0, rowSize);
        return copiedDateValues;
      case INT64:
      case TIMESTAMP:
        final long[] longValues = (long[]) valueList;
        final long[] copiedLongValues = new long[rowSize];
        System.arraycopy(longValues, 0, copiedLongValues, 0, rowSize);
        return copiedLongValues;
      case FLOAT:
        final float[] floatValues = (float[]) valueList;
        final float[] copiedFloatValues = new float[rowSize];
        System.arraycopy(floatValues, 0, copiedFloatValues, 0, rowSize);
        return copiedFloatValues;
      case DOUBLE:
        final double[] doubleValues = (double[]) valueList;
        final double[] copiedDoubleValues = new double[rowSize];
        System.arraycopy(doubleValues, 0, copiedDoubleValues, 0, rowSize);
        return copiedDoubleValues;
      case TEXT:
      case BLOB:
      case STRING:
        final Binary[] binaryValues = (Binary[]) valueList;
        final Binary[] copiedBinaryValues = new Binary[rowSize];
        System.arraycopy(binaryValues, 0, copiedBinaryValues, 0, rowSize);
        return copiedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }
}
