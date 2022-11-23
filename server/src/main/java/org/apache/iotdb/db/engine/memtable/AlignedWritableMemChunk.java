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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AlignedWritableMemChunk implements IWritableMemChunk {

  private final Map<String, Integer> measurementIndexMap;
  private final List<IMeasurementSchema> schemaList;
  private AlignedTVList list;

  private static final int maxNumberOfPointsInPage =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(AlignedWritableMemChunk.class);

  public AlignedWritableMemChunk(List<IMeasurementSchema> schemaList) {
    this.measurementIndexMap = new LinkedHashMap<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementId(), i);
      dataTypeList.add(schemaList.get(i).getType());
    }
    this.list = AlignedTVList.newAlignedList(dataTypeList);
  }

  private AlignedWritableMemChunk(List<IMeasurementSchema> schemaList, AlignedTVList list) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementId(), i);
    }
    this.list = list;
  }

  public Set<String> getAllMeasurements() {
    return measurementIndexMap.keySet();
  }

  public boolean containsMeasurement(String measurementId) {
    return measurementIndexMap.containsKey(measurementId);
  }

  @Override
  public void putLong(long t, long v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putInt(long t, int v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putFloat(long t, float v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putDouble(long t, double v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBinaryWithFlushCheck(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putAlignedValueWithFlushCheck(long t, Object[] v, int[] columnIndexArray) {
    list.putAlignedValue(t, v, columnIndexArray);
    return list.reachMaxChunkSizeThreshold();
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBinariesWithFlushCheck(
      long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int[] columnIndexArray, int start, int end) {
    list.putAlignedValues(t, v, bitMaps, columnIndexArray, start, end);
    return list.reachMaxChunkSizeThreshold();
  }

  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    int[] columnIndexArray = checkColumnsInInsertPlan(schemaList);
    return putAlignedValueWithFlushCheck(insertTime, objectValue, columnIndexArray);
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean writeAlignedValuesWithFlushCheck(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    int[] columnIndexArray = checkColumnsInInsertPlan(schemaList);
    return putAlignedValuesWithFlushCheck(times, valueList, bitMaps, columnIndexArray, start, end);
  }

  /**
   * Check schema of columns and return array that mapping existed schema to index of data column
   *
   * @param schemaListInInsertPlan Contains all existed schema in InsertPlan. If some timeseries
   *     have been deleted, there will be null in its slot.
   * @return columnIndexArray: schemaList[i] is schema of columns[columnIndexArray[i]]
   */
  private int[] checkColumnsInInsertPlan(List<IMeasurementSchema> schemaListInInsertPlan) {
    Map<String, Integer> measurementIdsInInsertPlan = new HashMap<>();
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      if (schemaListInInsertPlan.get(i) != null) {
        measurementIdsInInsertPlan.put(schemaListInInsertPlan.get(i).getMeasurementId(), i);
        if (!containsMeasurement(schemaListInInsertPlan.get(i).getMeasurementId())) {
          this.measurementIndexMap.put(
              schemaListInInsertPlan.get(i).getMeasurementId(), measurementIndexMap.size());
          this.schemaList.add(schemaListInInsertPlan.get(i));
          this.list.extendColumn(schemaListInInsertPlan.get(i).getType());
        }
      }
    }
    int[] columnIndexArray = new int[measurementIndexMap.size()];
    measurementIndexMap.forEach(
        (measurementId, i) ->
            columnIndexArray[i] = measurementIdsInInsertPlan.getOrDefault(measurementId, -1));
    return columnIndexArray;
  }

  @Override
  public TVList getTVList() {
    return list;
  }

  @Override
  public long count() {
    return (long) list.rowCount() * measurementIndexMap.size();
  }

  public long alignedListSize() {
    return list.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return null;
  }

  @Override
  public long getMaxTime() {
    return list.getMaxTime();
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(List<IMeasurementSchema> schemaList) {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    List<Integer> columnIndexList = new ArrayList<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(
          measurementIndexMap.getOrDefault(measurementSchema.getMeasurementId(), -1));
      dataTypeList.add(measurementSchema.getType());
    }
    return list.getTvListByColumnIndex(columnIndexList, dataTypeList);
  }

  private void sortTVList() {
    // check reference count
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      list = list.clone();
    }

    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public synchronized void sortTvListForFlush() {
    sortTVList();
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    return list.delete(lowerBound, upperBound);
  }

  public Pair<Integer, Boolean> deleteDataFromAColumn(
      long lowerBound, long upperBound, String measurementId) {
    return list.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
  }

  public void removeColumn(String measurementId) {
    list.deleteColumn(measurementIndexMap.get(measurementId));
    IMeasurementSchema schemaToBeRemoved = schemaList.get(measurementIndexMap.get(measurementId));
    schemaList.remove(schemaToBeRemoved);
    measurementIndexMap.clear();
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementId(), i);
    }
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schemaList);
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;

    boolean[] timeDuplicateInfo = null;
    List<Integer> pageRange = new ArrayList<>();
    int range = 0;
    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);

      if (sortedRowIndex == list.rowCount() - 1 || time != list.getTime(sortedRowIndex + 1)) {
        if (range == 0) {
          pageRange.add(sortedRowIndex);
        }
        range++;
        if (range == maxNumberOfPointsInPage) {
          pageRange.add(sortedRowIndex);
          range = 0;
        }
      } else {
        if (Objects.isNull(timeDuplicateInfo)) {
          timeDuplicateInfo = new boolean[list.rowCount()];
        }
        timeDuplicateInfo[sortedRowIndex] = true;
      }
    }

    if (range != 0) {
      pageRange.add(list.rowCount() - 1);
    }

    List<TSDataType> dataTypes = list.getTsDataTypes();
    for (int pageNum = 0; pageNum < pageRange.size() / 2; pageNum += 1) {
      for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
        // Pair of Time and Index
        Pair<Long, Integer> lastValidPointIndexForTimeDupCheck = null;
        if (Objects.nonNull(timeDuplicateInfo)) {
          lastValidPointIndexForTimeDupCheck = new Pair<>(Long.MIN_VALUE, null);
        }
        for (int sortedRowIndex = pageRange.get(pageNum * 2);
            sortedRowIndex <= pageRange.get(pageNum * 2 + 1);
            sortedRowIndex++) {

          // skip time duplicated rows
          long time = list.getTime(sortedRowIndex);
          if (Objects.nonNull(timeDuplicateInfo)) {
            if (!list.isNullValue(list.getValueIndex(sortedRowIndex), columnIndex)) {
              lastValidPointIndexForTimeDupCheck.left = time;
              lastValidPointIndexForTimeDupCheck.right = list.getValueIndex(sortedRowIndex);
            }
            if (timeDuplicateInfo[sortedRowIndex]) {
              continue;
            }
          }

          // The part of code solves the following problem:
          // Time: 1,2,2,3
          // Value: 1,2,null,null
          // When rowIndex:1, pair(min,null), timeDuplicateInfo:false, write(T:1,V:1)
          // When rowIndex:2, pair(2,2), timeDuplicateInfo:true, skip writing value
          // When rowIndex:3, pair(2,2), timeDuplicateInfo:false, T:2!=air.left:2, write(T:2,V:2)
          // When rowIndex:4, pair(2,2), timeDuplicateInfo:false, T:3!=pair.left:2,
          // write(T:3,V:null)

          int originRowIndex;
          if (Objects.nonNull(lastValidPointIndexForTimeDupCheck)
              && (time == lastValidPointIndexForTimeDupCheck.left)) {
            originRowIndex = lastValidPointIndexForTimeDupCheck.right;
          } else {
            originRowIndex = list.getValueIndex(sortedRowIndex);
          }

          boolean isNull = list.isNullValue(originRowIndex, columnIndex);
          switch (dataTypes.get(columnIndex)) {
            case BOOLEAN:
              alignedChunkWriter.writeByColumn(
                  time, list.getBooleanByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case INT32:
              alignedChunkWriter.writeByColumn(
                  time, list.getIntByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case INT64:
              alignedChunkWriter.writeByColumn(
                  time, list.getLongByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case FLOAT:
              alignedChunkWriter.writeByColumn(
                  time, list.getFloatByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case DOUBLE:
              alignedChunkWriter.writeByColumn(
                  time, list.getDoubleByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case TEXT:
              alignedChunkWriter.writeByColumn(
                  time, list.getBinaryByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            default:
              break;
          }
        }
        alignedChunkWriter.nextColumn();
      }

      long[] times = new long[maxNumberOfPointsInPage];
      int pointsInPage = 0;
      for (int sortedRowIndex = pageRange.get(pageNum * 2);
          sortedRowIndex <= pageRange.get(pageNum * 2 + 1);
          sortedRowIndex++) {
        if (Objects.isNull(timeDuplicateInfo) || !timeDuplicateInfo[sortedRowIndex]) {
          times[pointsInPage++] = list.getTime(sortedRowIndex);
        }
      }

      alignedChunkWriter.write(times, pointsInPage, 0);
    }
  }

  @Override
  public void release() {
    if (list.getReferenceCount() == 0) {
      list.clear();
    }
  }

  @Override
  public long getFirstPoint() {
    if (list.rowCount() == 0) {
      return Long.MAX_VALUE;
    }
    return getSortedTvListForQuery().getTimeValuePair(0).getTimestamp();
  }

  @Override
  public long getLastPoint() {
    if (list.rowCount() == 0) {
      return Long.MIN_VALUE;
    }
    return getSortedTvListForQuery()
        .getTimeValuePair(getSortedTvListForQuery().rowCount() - 1)
        .getTimestamp();
  }

  @Override
  public boolean isEmpty() {
    return list.rowCount() == 0;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (IMeasurementSchema schema : schemaList) {
      size += schema.serializedSize();
    }

    size += list.serializedSize();
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(schemaList.size(), buffer);
    for (IMeasurementSchema schema : schemaList) {
      byte[] bytes = new byte[schema.serializedSize()];
      schema.serializeTo(ByteBuffer.wrap(bytes));
      buffer.put(bytes);
    }

    list.serializeToWAL(buffer);
  }

  public static AlignedWritableMemChunk deserialize(DataInputStream stream) throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList list = (AlignedTVList) TVList.deserialize(stream);
    return new AlignedWritableMemChunk(schemaList, list);
  }
}
