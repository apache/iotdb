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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.MergeSortAlignedTVListIterator;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AlignedWritableMemChunk implements IWritableMemChunk {

  private final Map<String, Integer> measurementIndexMap;
  private final List<IMeasurementSchema> schemaList;
  private AlignedTVList list;
  private List<AlignedTVList> sortedList;
  private boolean ignoreAllNullRows;

  private static final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  public AlignedWritableMemChunk(List<IMeasurementSchema> schemaList, boolean isTableModel) {
    this.measurementIndexMap = new LinkedHashMap<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
      dataTypeList.add(schemaList.get(i).getType());
    }
    this.list = AlignedTVList.newAlignedList(dataTypeList);
    this.sortedList = new ArrayList<>();
    this.ignoreAllNullRows = !isTableModel;
  }

  private AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList, AlignedTVList list, boolean isTableModel) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
    this.list = list;
    this.sortedList = new ArrayList<>();
    this.ignoreAllNullRows = !isTableModel;
  }

  public Set<String> getAllMeasurements() {
    return measurementIndexMap.keySet();
  }

  public boolean containsMeasurement(String measurementId) {
    return measurementIndexMap.containsKey(measurementId);
  }

  @Override
  public boolean putLongWithFlushCheck(long t, long v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putIntWithFlushCheck(long t, int v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putFloatWithFlushCheck(long t, float v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putDoubleWithFlushCheck(long t, double v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBinaryWithFlushCheck(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBooleanWithFlushCheck(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putAlignedValueWithFlushCheck(long t, Object[] v) {
    synchronized (list) {
      list.putAlignedValue(t, v);
      return list.reachChunkSizeOrPointNumThreshold();
    }
  }

  @Override
  public boolean putLongsWithFlushCheck(long[] t, long[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putIntsWithFlushCheck(long[] t, int[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putFloatsWithFlushCheck(long[] t, float[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putDoublesWithFlushCheck(long[] t, double[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBinariesWithFlushCheck(
      long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBooleansWithFlushCheck(
      long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    synchronized (list) {
      list.putAlignedValues(t, v, bitMaps, start, end, results);
      return list.reachChunkSizeOrPointNumThreshold();
    }
  }

  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  protected void handoverAlignedTvList() {
    // ensure query contexts won't be removed from list during handover process.
    list.lockQueryList();
    try {
      if (list.isSorted()) {
        sortedList.add(list);
        return;
      }

      if (list.getQueryContextList().isEmpty()) {
        list.safelySort();
        sortedList.add(list);
      } else {
        QueryContext firstQuery = list.getQueryContextList().get(0);
        // reserve query memory
        if (firstQuery instanceof FragmentInstanceContext) {
          MemoryReservationManager memoryReservationManager =
              ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
          memoryReservationManager.reserveMemoryCumulatively(list.calculateRamSize());
        }
        // update current TVList owner to first query in the list
        list.setOwnerQuery(firstQuery);
        // clone tv list
        AlignedTVList cloneList = list.clone();
        sortedList.add(cloneList);
      }
    } finally {
      list.unlockQueryList();
    }
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (IMeasurementSchema schema : schemaList) {
      dataTypeList.add(schema.getType());
    }
    this.list = AlignedTVList.newAlignedList(dataTypeList);
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    Object[] reorderedValue =
        checkAndReorderColumnValuesInInsertPlan(schemaList, objectValue, null).left;
    if (putAlignedValueWithFlushCheck(insertTime, reorderedValue)) {
      return true;
    }
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
    return false;
  }

  @Override
  public boolean writeAlignedValuesWithFlushCheck(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    Pair<Object[], BitMap[]> pair =
        checkAndReorderColumnValuesInInsertPlan(schemaList, valueList, bitMaps);
    Object[] reorderedColumnValues = pair.left;
    BitMap[] reorderedBitMaps = pair.right;
    if (putAlignedValuesWithFlushCheck(
        times, reorderedColumnValues, reorderedBitMaps, start, end, results)) {
      return true;
    }
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
    return false;
  }

  /**
   * Check metadata of columns and return array that mapping existed metadata to index of data
   * column.
   *
   * @param schemaListInInsertPlan Contains all existed schema in InsertPlan. If some timeseries
   *     have been deleted, there will be null in its slot.
   * @return columnIndexArray: schemaList[i] is schema of columns[columnIndexArray[i]]
   */
  private Pair<Object[], BitMap[]> checkAndReorderColumnValuesInInsertPlan(
      List<IMeasurementSchema> schemaListInInsertPlan, Object[] columnValues, BitMap[] bitMaps) {
    Object[] reorderedColumnValues = new Object[schemaList.size()];
    BitMap[] reorderedBitMaps = bitMaps == null ? null : new BitMap[schemaList.size()];
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      IMeasurementSchema measurementSchema = schemaListInInsertPlan.get(i);
      if (measurementSchema != null) {
        Integer index = this.measurementIndexMap.get(measurementSchema.getMeasurementName());
        // Index is null means this measurement was not in this AlignedTVList before.
        // We need to extend a new column in AlignedMemChunk and AlignedTVList.
        // And the reorderedColumnValues should extend one more column for the new measurement
        if (index == null) {
          index = measurementIndexMap.size();
          this.measurementIndexMap.put(schemaListInInsertPlan.get(i).getMeasurementName(), index);
          this.schemaList.add(schemaListInInsertPlan.get(i));
          this.list.extendColumn(schemaListInInsertPlan.get(i).getType());
          reorderedColumnValues =
              Arrays.copyOf(reorderedColumnValues, reorderedColumnValues.length + 1);
          if (reorderedBitMaps != null) {
            reorderedBitMaps = Arrays.copyOf(reorderedBitMaps, reorderedBitMaps.length + 1);
          }
        }
        reorderedColumnValues[index] = columnValues[i];
        if (bitMaps != null) {
          reorderedBitMaps[index] = bitMaps[i];
        }
      }
    }
    return new Pair<>(reorderedColumnValues, reorderedBitMaps);
  }

  @Override
  public AlignedTVList getWorkingTVList() {
    return list;
  }

  public void setWorkingTVList(AlignedTVList list) {
    this.list = list;
  }

  @Override
  public long count() {
    if (!ignoreAllNullRows && measurementIndexMap.isEmpty()) {
      return rowCount();
    }
    return rowCount() * measurementIndexMap.size();
  }

  @Override
  public long rowCount() {
    return alignedListSize();
  }

  public int alignedListSize() {
    int rowCount = list.rowCount();
    for (AlignedTVList alignedTvList : sortedList) {
      rowCount += alignedTvList.rowCount();
    }
    return rowCount;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return null;
  }

  @Override
  public long getMaxTime() {
    long maxTime = list.getMaxTime();
    for (AlignedTVList alignedTvList : sortedList) {
      maxTime = Math.max(maxTime, alignedTvList.getMaxTime());
    }
    return maxTime;
  }

  @Override
  public long getMinTime() {
    long minTime = list.getMinTime();
    for (AlignedTVList alignedTvList : sortedList) {
      minTime = Math.min(minTime, alignedTvList.getMinTime());
    }
    return minTime;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(
      List<IMeasurementSchema> schemaList, boolean ignoreAllNullRows) {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    List<Integer> columnIndexList = new ArrayList<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(
          measurementIndexMap.getOrDefault(measurementSchema.getMeasurementName(), -1));
      dataTypeList.add(measurementSchema.getType());
    }
    return list.getTvListByColumnIndex(columnIndexList, dataTypeList, ignoreAllNullRows);
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
    if (!list.isSorted()) {
      list.safelySort();
    }
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = list.delete(lowerBound, upperBound);
    for (AlignedTVList alignedTvList : sortedList) {
      deletedNumber += alignedTvList.delete(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  public int deleteTime(long lowerBound, long upperBound) {
    int deletedNumber = list.deleteTime(lowerBound, upperBound);
    for (AlignedTVList alignedTvList : sortedList) {
      deletedNumber += alignedTvList.deleteTime(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  public Pair<Integer, Boolean> deleteDataFromAColumn(
      long lowerBound, long upperBound, String measurementId) {
    Pair<Integer, Boolean> deletePair =
        list.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
    for (AlignedTVList alignedTvList : sortedList) {
      Pair<Integer, Boolean> p =
          alignedTvList.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
      deletePair.left += p.left;
      deletePair.right = deletePair.right && p.right;
    }
    return deletePair;
  }

  public void removeColumn(String measurementId) {
    list.deleteColumn(measurementIndexMap.get(measurementId));
    for (AlignedTVList alignedTvList : sortedList) {
      alignedTvList.deleteColumn(measurementIndexMap.get(measurementId));
    }
    IMeasurementSchema schemaToBeRemoved = schemaList.get(measurementIndexMap.get(measurementId));
    schemaList.remove(schemaToBeRemoved);
    measurementIndexMap.clear();
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schemaList);
  }

  private void encodeWorkingAlignedTVList(IChunkWriter chunkWriter) {
    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;

    BitMap allValueColDeletedMap;
    allValueColDeletedMap = ignoreAllNullRows ? list.getAllValueColDeletedMap() : null;
    boolean[] timeDuplicateInfo = null;
    List<Integer> pageRange = new ArrayList<>();
    int range = 0;
    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);
      if (range == 0) {
        pageRange.add(sortedRowIndex);
      }
      range++;
      if (range == MAX_NUMBER_OF_POINTS_IN_PAGE) {
        pageRange.add(sortedRowIndex);
        range = 0;
      }

      int nextRowIndex = sortedRowIndex + 1;
      while (nextRowIndex < list.rowCount()
          && ((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(list.getValueIndex(nextRowIndex)))
              || list.isTimeDeleted(nextRowIndex))) {
        nextRowIndex++;
      }
      if (nextRowIndex != list.rowCount() && time == list.getTime(nextRowIndex)) {
        if (Objects.isNull(timeDuplicateInfo)) {
          timeDuplicateInfo = new boolean[list.rowCount()];
        }
        timeDuplicateInfo[sortedRowIndex] = true;
      }
      sortedRowIndex = nextRowIndex - 1;
    }

    if (range != 0) {
      pageRange.add(list.rowCount() - 1);
    }

    List<TSDataType> dataTypes = list.getTsDataTypes();
    Pair<Long, Integer>[] lastValidPointIndexForTimeDupCheck = new Pair[dataTypes.size()];

    for (int pageNum = 0; pageNum < pageRange.size() / 2; pageNum += 1) {
      for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
        // Pair of Time and Index
        if (Objects.nonNull(timeDuplicateInfo)
            && lastValidPointIndexForTimeDupCheck[columnIndex] == null) {
          lastValidPointIndexForTimeDupCheck[columnIndex] = new Pair<>(Long.MIN_VALUE, null);
        }
        TSDataType tsDataType = dataTypes.get(columnIndex);
        for (int sortedRowIndex = pageRange.get(pageNum * 2);
            sortedRowIndex <= pageRange.get(pageNum * 2 + 1);
            sortedRowIndex++) {
          // skip empty row
          if (((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(list.getValueIndex(sortedRowIndex)))
              || (list.isTimeDeleted(sortedRowIndex)))) {
            continue;
          }
          // skip time duplicated rows
          long time = list.getTime(sortedRowIndex);
          if (Objects.nonNull(timeDuplicateInfo)) {
            if (!list.isNullValue(list.getValueIndex(sortedRowIndex), columnIndex)) {
              lastValidPointIndexForTimeDupCheck[columnIndex].left = time;
              lastValidPointIndexForTimeDupCheck[columnIndex].right =
                  list.getValueIndex(sortedRowIndex);
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
          if (Objects.nonNull(lastValidPointIndexForTimeDupCheck[columnIndex])
              && (time == lastValidPointIndexForTimeDupCheck[columnIndex].left)) {
            originRowIndex = lastValidPointIndexForTimeDupCheck[columnIndex].right;
          } else {
            originRowIndex = list.getValueIndex(sortedRowIndex);
          }

          boolean isNull = list.isNullValue(originRowIndex, columnIndex);
          switch (tsDataType) {
            case BOOLEAN:
              alignedChunkWriter.writeByColumn(
                  time, list.getBooleanByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case INT32:
            case DATE:
              alignedChunkWriter.writeByColumn(
                  time, list.getIntByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            case INT64:
            case TIMESTAMP:
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
            case BLOB:
            case STRING:
              alignedChunkWriter.writeByColumn(
                  time, list.getBinaryByValueIndex(originRowIndex, columnIndex), isNull);
              break;
            default:
              break;
          }
        }
        alignedChunkWriter.nextColumn();
      }

      long[] times = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];
      int pointsInPage = 0;
      for (int sortedRowIndex = pageRange.get(pageNum * 2);
          sortedRowIndex <= pageRange.get(pageNum * 2 + 1);
          sortedRowIndex++) {
        // skip empty row
        if (((allValueColDeletedMap != null
                && allValueColDeletedMap.isMarked(list.getValueIndex(sortedRowIndex)))
            || (list.isTimeDeleted(sortedRowIndex)))) {
          continue;
        }
        if (Objects.isNull(timeDuplicateInfo) || !timeDuplicateInfo[sortedRowIndex]) {
          times[pointsInPage++] = list.getTime(sortedRowIndex);
        }
      }

      alignedChunkWriter.write(times, pointsInPage, 0);
    }
  }

  @SuppressWarnings({"squid:S6541", "squid:S3776"})
  @Override
  public void encode(IChunkWriter chunkWriter) {
    if (TVLIST_SORT_THRESHOLD == 0) {
      encodeWorkingAlignedTVList(chunkWriter);
      return;
    }

    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;
    // create MergeSortAlignedTVListIterator.
    List<AlignedTVList> alignedTvLists = new ArrayList<>(sortedList);
    alignedTvLists.add(list);
    MergeSortAlignedTVListIterator timeValuePairIterator =
        new MergeSortAlignedTVListIterator(
            alignedTvLists, null, null, null, null, null, ignoreAllNullRows);

    int pointsInPage = 0;
    long[] times = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      times[pointsInPage] = tvPair.getTimestamp();
      TsPrimitiveType[] values = tvPair.getValue().getVector();
      for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
        ValueChunkWriter valueChunkWriter =
            alignedChunkWriter.getValueChunkWriterByIndex(columnIndex);
        boolean isNull = values[columnIndex].getValue() == null;
        switch (schemaList.get(columnIndex).getType()) {
          case BOOLEAN:
            valueChunkWriter.write(tvPair.getTimestamp(), values[columnIndex].getBoolean(), isNull);
            break;
          case INT32:
          case DATE:
            valueChunkWriter.write(tvPair.getTimestamp(), values[columnIndex].getInt(), isNull);
            break;
          case INT64:
          case TIMESTAMP:
            valueChunkWriter.write(tvPair.getTimestamp(), values[columnIndex].getLong(), isNull);
            break;
          case FLOAT:
            valueChunkWriter.write(tvPair.getTimestamp(), values[columnIndex].getFloat(), isNull);
            break;
          case DOUBLE:
            valueChunkWriter.write(tvPair.getTimestamp(), values[columnIndex].getDouble(), isNull);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            valueChunkWriter.write(tvPair.getTimestamp(), values[columnIndex].getBinary(), isNull);
            break;
          default:
            break;
        }
      }
      pointsInPage++;
      if (pointsInPage % MAX_NUMBER_OF_POINTS_IN_PAGE == 0) {
        alignedChunkWriter.write(times, pointsInPage, 0);
        pointsInPage = 0;
      }
    }
  }

  private void maybeReleaseTvList(AlignedTVList alignedTvList) {
    alignedTvList.lockQueryList();
    try {
      if (alignedTvList.getQueryContextList().isEmpty()) {
        alignedTvList.clear();
      } else {
        QueryContext firstQuery = alignedTvList.getQueryContextList().get(0);
        // transfer memory from write process to read process. Here it reserves read memory and
        // releaseFlushedMemTable will release write memory.
        if (firstQuery instanceof FragmentInstanceContext) {
          MemoryReservationManager memoryReservationManager =
              ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
          memoryReservationManager.reserveMemoryCumulatively(alignedTvList.calculateRamSize());
        }
        // update current TVList owner to first query in the list
        alignedTvList.setOwnerQuery(firstQuery);
      }
    } finally {
      alignedTvList.unlockQueryList();
    }
  }

  @Override
  public void release() {
    maybeReleaseTvList(list);
    for (AlignedTVList alignedTvList : sortedList) {
      maybeReleaseTvList(alignedTvList);
    }
  }

  @Override
  public long getFirstPoint() {
    if (rowCount() == 0) {
      return Long.MAX_VALUE;
    }
    return getMinTime();
  }

  @Override
  public long getLastPoint() {
    if (rowCount() == 0) {
      return Long.MIN_VALUE;
    }
    return getMaxTime();
  }

  @Override
  public boolean isEmpty() {
    return rowCount() == 0;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (IMeasurementSchema schema : schemaList) {
      size += schema.serializedSize();
    }

    for (AlignedTVList alignedTvList : sortedList) {
      size += alignedTvList.serializedSize();
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
    for (AlignedTVList alignedTvList : sortedList) {
      alignedTvList.serializeToWAL(buffer);
    }
    list.serializeToWAL(buffer);
  }

  public static AlignedWritableMemChunk deserialize(DataInputStream stream, boolean isTableModel)
      throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList list = (AlignedTVList) TVList.deserialize(stream);
    return new AlignedWritableMemChunk(schemaList, list, isTableModel);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public List<AlignedTVList> getSortedList() {
    return sortedList;
  }

  public List<Integer> getColumnIndexList(List<IMeasurementSchema> schemaList) {
    List<Integer> columnIndexList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(
          measurementIndexMap.getOrDefault(measurementSchema.getMeasurementName(), -1));
    }
    return columnIndexList;
  }
}
