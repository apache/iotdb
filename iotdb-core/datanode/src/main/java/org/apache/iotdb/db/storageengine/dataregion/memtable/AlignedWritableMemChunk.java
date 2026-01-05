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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
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
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public class AlignedWritableMemChunk extends AbstractWritableMemChunk {

  private Map<String, Integer> measurementIndexMap;
  private List<TSDataType> dataTypes;
  private final List<IMeasurementSchema> schemaList;
  private AlignedTVList list;
  private List<AlignedTVList> sortedList;
  private long sortedRowCount = 0;
  private final boolean ignoreAllNullRows;

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final int TVLIST_SORT_THRESHOLD = CONFIG.getTvListSortThreshold();

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  private EncryptParameter encryptParameter;

  public AlignedWritableMemChunk(List<IMeasurementSchema> schemaList, boolean isTableModel) {
    this(schemaList, isTableModel, EncryptUtils.getEncryptParameter());
  }

  public AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList,
      boolean isTableModel,
      EncryptParameter encryptParameter) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.dataTypes = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
      dataTypes.add(schemaList.get(i).getType());
    }
    this.list = AlignedTVList.newAlignedList(dataTypes);
    this.sortedList = new ArrayList<>();
    this.ignoreAllNullRows = !isTableModel;
    this.encryptParameter = encryptParameter;
  }

  private AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList, AlignedTVList list, boolean isTableModel) {
    this(schemaList, list, isTableModel, EncryptUtils.getEncryptParameter());
  }

  private AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList,
      AlignedTVList list,
      boolean isTableModel,
      EncryptParameter encryptParameter) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
    this.list = list;
    this.dataTypes = list.getTsDataTypes();
    this.sortedList = new ArrayList<>();
    this.ignoreAllNullRows = !isTableModel;
    this.encryptParameter = encryptParameter;
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
  public void putBinary(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putAlignedRow(long t, Object[] v) {
    list.putAlignedValue(t, v);
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
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putAlignedTablet(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    list.putAlignedValues(t, v, bitMaps, start, end, results);
  }

  @Override
  public void writeNonAlignedPoint(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void writeNonAlignedTablet(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  protected void handoverAlignedTvList() {
    if (!list.isSorted()) {
      list.sort();
    }
    sortedList.add(list);
    this.sortedRowCount += list.rowCount();
    this.list = AlignedTVList.newAlignedList(new ArrayList<>(dataTypes));
    this.dataTypes = list.getTsDataTypes();
  }

  @Override
  public void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    Object[] reorderedValue =
        checkAndReorderColumnValuesInInsertPlan(schemaList, objectValue, null).left;
    putAlignedRow(insertTime, reorderedValue);
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
  }

  @Override
  public void writeAlignedTablet(
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
    putAlignedTablet(times, reorderedColumnValues, reorderedBitMaps, start, end, results);
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
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
          index = this.list.getTsDataTypes().size();
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

  public long[] getAnySatisfiedTimestamp(
      List<List<TimeRange>> deletionList,
      List<BitMap> bitMaps,
      boolean ignoreAllNullRows,
      Filter globalTimeFilter) {
    BitMap columnHasNonNullValue = new BitMap(schemaList.size());
    AtomicInteger hasNonNullValueColumnCount = new AtomicInteger(0);
    Map<Long, BitMap> timestampWithBitmap = new TreeMap<>();

    getAnySatisfiedTimestamp(
        list,
        deletionList,
        ignoreAllNullRows,
        timestampWithBitmap,
        globalTimeFilter,
        columnHasNonNullValue,
        hasNonNullValueColumnCount);
    for (int i = 0;
        i < sortedList.size() && hasNonNullValueColumnCount.get() < schemaList.size();
        i++) {
      if (!ignoreAllNullRows && !timestampWithBitmap.isEmpty()) {
        // count devices in table model
        break;
      }
      getAnySatisfiedTimestamp(
          sortedList.get(i),
          deletionList,
          ignoreAllNullRows,
          timestampWithBitmap,
          globalTimeFilter,
          columnHasNonNullValue,
          hasNonNullValueColumnCount);
    }

    long[] timestamps = new long[timestampWithBitmap.size()];
    int idx = 0;
    for (Map.Entry<Long, BitMap> entry : timestampWithBitmap.entrySet()) {
      timestamps[idx++] = entry.getKey();
      bitMaps.add(entry.getValue());
    }
    return timestamps;
  }

  private void getAnySatisfiedTimestamp(
      AlignedTVList alignedTVList,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      Map<Long, BitMap> timestampWithBitmap,
      Filter globalTimeFilter,
      BitMap columnHasNonNullValue,
      AtomicInteger hasNonNullValueColumnCount) {
    if (globalTimeFilter != null
        && !globalTimeFilter.satisfyStartEndTime(
            alignedTVList.getMinTime(), alignedTVList.getMaxTime())) {
      return;
    }
    BitMap allValueColDeletedMap = alignedTVList.getAllValueColDeletedMap();
    int rowCount = alignedTVList.rowCount();
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    // example:
    // globalTimeFilter:null, ignoreAllNullRows: true
    // tvList:
    // time s1    s2    s3
    // 1    1     null  null
    // 2    null  1     null
    // 2    1     1     null
    // 3    1     null  null
    // 4    1     null  1
    // timestampWithBitmap:
    // timestamp: 1 bitmap: 011
    // timestamp: 2 bitmap: 101
    // timestamp: 4 bitmap: 110
    List<long[]> timestampsList = alignedTVList.getTimestamps();
    List<int[]> indicesList = alignedTVList.getIndices();
    int row = -1;
    for (int i = 0; i < timestampsList.size(); i++) {
      long[] timestamps = timestampsList.get(i);
      int[] indices = indicesList == null ? null : indicesList.get(i);
      int limit = (i == timestampsList.size() - 1) ? rowCount - i * ARRAY_SIZE : ARRAY_SIZE;
      for (int j = 0; j < limit; j++) {
        row++;
        // the row is deleted
        if (allValueColDeletedMap != null && allValueColDeletedMap.isMarked(row)) {
          continue;
        }
        long timestamp = timestamps[j];
        if (globalTimeFilter != null && !globalTimeFilter.satisfy(timestamp, null)) {
          continue;
        }

        // Note that this method will only perform bitmap unmarking on the first occurrence of a
        // non-null value in multiple timestamps for the same column.
        BitMap currentRowNullValueBitmap = null;
        for (int column = 0; column < schemaList.size(); column++) {
          if (alignedTVList.isNullValue(indices == null ? row : indices[j], column)) {
            continue;
          }

          // skip deleted row
          if (valueColumnsDeletionList != null && !valueColumnsDeletionList.isEmpty()) {
            List<TimeRange> columnDeletionList = valueColumnsDeletionList.get(column);
            int[] deleteCursor = valueColumnDeleteCursor.get(column);
            if (columnDeletionList != null && !columnDeletionList.isEmpty()) {
              if (!alignedTVList.isSorted()) {
                deleteCursor[0] = 0;
              }
              if (isPointDeleted(timestamp, columnDeletionList, deleteCursor)) {
                continue;
              }
            }
          }
          if (!columnHasNonNullValue.isMarked(column)) {
            hasNonNullValueColumnCount.incrementAndGet();
            columnHasNonNullValue.mark(column);
            currentRowNullValueBitmap =
                currentRowNullValueBitmap != null
                    ? currentRowNullValueBitmap
                    : timestampWithBitmap.computeIfAbsent(
                        timestamp, k -> getAllMarkedBitmap(schemaList.size()));
            currentRowNullValueBitmap.unmark(column);
          }
        }

        if (!ignoreAllNullRows) {
          timestampWithBitmap.put(
              timestamp,
              currentRowNullValueBitmap != null
                  ? currentRowNullValueBitmap
                  : getAllMarkedBitmap(schemaList.size()));
          return;
        }
        if (currentRowNullValueBitmap == null) {
          continue;
        }
        // found new column with non-null value
        timestampWithBitmap.put(timestamp, currentRowNullValueBitmap);

        if (hasNonNullValueColumnCount.get() == schemaList.size()) {
          return;
        }
      }
    }
  }

  private BitMap getAllMarkedBitmap(int size) {
    BitMap bitMap = new BitMap(size);
    bitMap.markAll();
    return bitMap;
  }

  @Override
  public AlignedTVList getWorkingTVList() {
    return list;
  }

  @Override
  public void setWorkingTVList(TVList list) {
    this.list = (AlignedTVList) list;
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
    return sortedRowCount + list.rowCount();
  }

  public int alignedListSize() {
    return (int) rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return null;
  }

  @Override
  public long getMaxTime() {
    if (isEmpty()) {
      return Long.MIN_VALUE;
    }
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
  public synchronized void sortTvListForFlush() {
    if (!list.isSorted()) {
      list.sort();
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
    int columnIndex = measurementIndexMap.get(measurementId);
    list.deleteColumn(columnIndex);
    for (AlignedTVList alignedTvList : sortedList) {
      alignedTvList.deleteColumn(columnIndex);
    }
    IMeasurementSchema schemaToBeRemoved = schemaList.get(columnIndex);
    measurementIndexMap.remove(schemaToBeRemoved.getMeasurementName());
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schemaList, encryptParameter);
  }

  @SuppressWarnings({"squid:S6541", "squid:S3776"})
  public void encodeWorkingAlignedTVList(
      BlockingQueue<Object> ioTaskQueue,
      long maxNumberOfPointsInChunk,
      int maxNumberOfPointsInPage) {
    BitMap allValueColDeletedMap;
    allValueColDeletedMap = ignoreAllNullRows ? list.getAllValueColDeletedMap() : null;

    boolean[] timeDuplicateInfo = null;

    List<List<Integer>> chunkRange = new ArrayList<>();
    // Eg. chunkRange: ((0,9,10,12),(13,15)) means this TVList contains 2 chunks,
    // 1st chunk contains 2 pages, 2nd chunk contains 1 page.
    List<Integer> pageRange = new ArrayList<>();

    int pointNumInPage = 0;
    int pointNumInChunk = 0;

    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);
      if (pointNumInPage == 0) {
        pageRange.add(sortedRowIndex);
      }
      pointNumInPage++;
      pointNumInChunk++;
      if (pointNumInPage == maxNumberOfPointsInPage) {
        pageRange.add(sortedRowIndex);
        pointNumInPage = 0;
      }
      if (pointNumInChunk >= maxNumberOfPointsInChunk) {
        if (pointNumInPage != 0) {
          pageRange.add(sortedRowIndex);
          pointNumInPage = 0;
        }
        chunkRange.add(pageRange);
        pageRange = new ArrayList<>();
        pointNumInChunk = 0;
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

    if (pointNumInPage != 0) {
      pageRange.add(list.rowCount() - 1);
    }
    if (pointNumInChunk != 0) {
      chunkRange.add(pageRange);
    }

    handleEncoding(
        ioTaskQueue, chunkRange, timeDuplicateInfo, allValueColDeletedMap, maxNumberOfPointsInPage);
  }

  private void handleEncoding(
      BlockingQueue<Object> ioTaskQueue,
      List<List<Integer>> chunkRange,
      boolean[] timeDuplicateInfo,
      BitMap allValueColDeletedMap,
      int maxNumberOfPointsInPage) {
    List<TSDataType> dataTypes = list.getTsDataTypes();
    Pair<Long, Integer>[] lastValidPointIndexForTimeDupCheck = new Pair[dataTypes.size()];
    for (List<Integer> pageRange : chunkRange) {
      AlignedChunkWriterImpl alignedChunkWriter =
          new AlignedChunkWriterImpl(schemaList, encryptParameter);
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
            if (allValueColDeletedMap != null
                && allValueColDeletedMap.isMarked(list.getValueIndex(sortedRowIndex))) {
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
            // When rowIndex:3, pair(2,2), timeDuplicateInfo:false, T:2==pair.left:2, write(T:2,V:2)
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
                    time,
                    !isNull && list.getBooleanByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case INT32:
              case DATE:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getIntByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case INT64:
              case TIMESTAMP:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getLongByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case FLOAT:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getFloatByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case DOUBLE:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getDoubleByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case TEXT:
              case STRING:
              case BLOB:
              case OBJECT:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? null : list.getBinaryByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              default:
                break;
            }
          }
          alignedChunkWriter.nextColumn();
        }

        long[] times = new long[Math.min(maxNumberOfPointsInPage, list.rowCount())];
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
      alignedChunkWriter.sealCurrentPage();
      alignedChunkWriter.clearPageWriter();
      try {
        ioTaskQueue.put(alignedChunkWriter);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public synchronized void encode(
      BlockingQueue<Object> ioTaskQueue, BatchEncodeInfo encodeInfo, long[] times) {
    encodeInfo.maxNumberOfPointsInChunk =
        Math.min(
            encodeInfo.maxNumberOfPointsInChunk,
            (encodeInfo.targetChunkSize / getAvgPointSizeOfLargestColumn()));

    if (TVLIST_SORT_THRESHOLD == 0) {
      encodeWorkingAlignedTVList(
          ioTaskQueue, encodeInfo.maxNumberOfPointsInChunk, encodeInfo.maxNumberOfPointsInPage);
      return;
    }

    AlignedChunkWriterImpl alignedChunkWriter =
        new AlignedChunkWriterImpl(schemaList, encryptParameter);

    // create MergeSortAlignedTVListIterator.
    List<AlignedTVList> alignedTvLists = new ArrayList<>(sortedList);
    alignedTvLists.add(list);
    List<Integer> columnIndexList = buildColumnIndexList(schemaList);
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(
            dataTypes,
            columnIndexList,
            alignedTvLists,
            ignoreAllNullRows,
            encodeInfo.maxNumberOfPointsInPage);

    while (timeValuePairIterator.hasNextBatch()) {
      timeValuePairIterator.encodeBatch(alignedChunkWriter, encodeInfo, times);
      if (encodeInfo.pointNumInPage >= encodeInfo.maxNumberOfPointsInPage) {
        alignedChunkWriter.write(times, encodeInfo.pointNumInPage, 0);
        encodeInfo.pointNumInPage = 0;
      }

      if (encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk) {
        alignedChunkWriter.sealCurrentPage();
        alignedChunkWriter.clearPageWriter();
        try {
          ioTaskQueue.put(alignedChunkWriter);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        alignedChunkWriter = new AlignedChunkWriterImpl(schemaList, encryptParameter);
        encodeInfo.reset();
      }
    }

    // last batch of points
    if (encodeInfo.pointNumInChunk > 0) {
      if (encodeInfo.pointNumInPage > 0) {
        alignedChunkWriter.write(times, encodeInfo.pointNumInPage, 0);
      }
      alignedChunkWriter.sealCurrentPage();
      alignedChunkWriter.clearPageWriter();
      try {
        ioTaskQueue.put(alignedChunkWriter);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      encodeInfo.reset();
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
    if (rowCount() == 0) {
      return true;
    }
    if (ignoreAllNullRows) {
      if (measurementIndexMap.isEmpty()) {
        return true;
      }

      if (list.rowCount() > 0) {
        BitMap allValueColDeletedMap = list.getAllValueColDeletedMap();
        if (allValueColDeletedMap == null || !allValueColDeletedMap.isAllMarked()) {
          return false;
        }
      }
      for (AlignedTVList alignedTvList : sortedList) {
        if (alignedTvList.rowCount() > 0) {
          BitMap allValueColDeletedMap = alignedTvList.getAllValueColDeletedMap();
          if (allValueColDeletedMap == null || !allValueColDeletedMap.isAllMarked()) {
            return false;
          }
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (IMeasurementSchema schema : schemaList) {
      size += schema.serializedSize();
    }
    size += Integer.BYTES;
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
    buffer.putInt(sortedList.size());
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
    int sortedListSize = stream.readInt();
    List<AlignedTVList> sortedList = new ArrayList<>();
    for (int i = 0; i < sortedListSize; i++) {
      AlignedTVList tvList = AlignedTVList.deserialize(stream);
      sortedList.add(tvList);
    }
    AlignedTVList list = AlignedTVList.deserialize(stream);
    AlignedWritableMemChunk chunk = new AlignedWritableMemChunk(schemaList, list, isTableModel);
    chunk.sortedList = sortedList;
    return chunk;
  }

  public static AlignedWritableMemChunk deserializeSingleTVListMemChunks(
      DataInputStream stream, boolean isTableModel) throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList list = AlignedTVList.deserialize(stream);
    return new AlignedWritableMemChunk(schemaList, list, isTableModel);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public boolean isAllDeleted() {
    if (!list.isAllDeleted()) {
      return false;
    }
    for (AlignedTVList alignedTvList : sortedList) {
      if (!alignedTvList.isAllDeleted()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<AlignedTVList> getSortedList() {
    return sortedList;
  }

  public List<Integer> buildColumnIndexList(List<IMeasurementSchema> schemaList) {
    List<Integer> columnIndexList = new ArrayList<>();
    for (IMeasurementSchema requiredMeasurementSchema : schemaList) {
      Integer measurementIndex =
          measurementIndexMap.get(requiredMeasurementSchema.getMeasurementName());
      if (measurementIndex == null) {
        columnIndexList.add(-1);
        continue;
      }
      IMeasurementSchema schemaInMemChunk = this.schemaList.get(measurementIndex);
      columnIndexList.add(
          requiredMeasurementSchema.getType().isCompatible(schemaInMemChunk.getType())
              ? measurementIndex
              : -1);
    }
    return columnIndexList;
  }

  public void checkDataType(InsertNode node) throws DataTypeInconsistentException {
    for (MeasurementSchema incomingSchema : node.getMeasurementSchemas()) {
      if (incomingSchema == null) {
        continue;
      }

      Integer index = measurementIndexMap.get(incomingSchema.getMeasurementName());
      if (index != null) {
        IMeasurementSchema existingSchema = schemaList.get(index);
        if (existingSchema.getType() != incomingSchema.getType()) {
          throw new DataTypeInconsistentException(
              existingSchema.getType(), incomingSchema.getType());
        }
      }
    }
  }

  // Choose maximum avgPointSizeOfLargestColumn among working and sorted AlignedTVList as
  // approximate calculation
  public int getAvgPointSizeOfLargestColumn() {
    int avgPointSizeOfLargestColumn = list.getAvgPointSizeOfLargestColumn();
    for (AlignedTVList alignedTVList : sortedList) {
      avgPointSizeOfLargestColumn =
          Math.max(avgPointSizeOfLargestColumn, alignedTVList.getAvgPointSizeOfLargestColumn());
    }
    return avgPointSizeOfLargestColumn;
  }

  @Override
  public void setEncryptParameter(EncryptParameter encryptParameter) {
    this.encryptParameter = encryptParameter;
  }
}
