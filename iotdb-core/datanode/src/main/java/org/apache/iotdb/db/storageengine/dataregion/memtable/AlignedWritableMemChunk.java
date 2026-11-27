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
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.TypeServices;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  // Note: Use AbstractWritableMemChunk.workingListForFlush instead of list in FlushTask
  private AlignedTVList list;
  private List<AlignedTVList> sortedList;
  private long sortedRowCount = 0;

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final int TVLIST_SORT_THRESHOLD = CONFIG.getTvListSortThreshold();

  private static final String UNSUPPORTED_TYPE = DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE;

  private EncryptParameter encryptParameter;

  /** Capacity reserved by a column container on its first materialization. */
  private int preallocatedBlockNum;

  public AlignedWritableMemChunk(List<IMeasurementSchema> schemaList) {
    this(schemaList, EncryptUtils.getEncryptParameter(), null);
  }

  public AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList, EncryptParameter encryptParameter) {
    this(schemaList, encryptParameter, null);
  }

  public AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList, EncryptParameter encryptParameter, String dataRegionId) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.dataTypes = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
      dataTypes.add(schemaList.get(i).getType());
    }
    this.preallocatedBlockNum = TVList.getDefaultArrayNum(dataRegionId);
    this.list = AlignedTVList.newAlignedList(dataTypes, true, preallocatedBlockNum);
    this.sortedList = new ArrayList<>();
    this.encryptParameter = encryptParameter;
  }

  private AlignedWritableMemChunk(List<IMeasurementSchema> schemaList, AlignedTVList list) {
    this(schemaList, list, EncryptUtils.getEncryptParameter());
  }

  private AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList, AlignedTVList list, EncryptParameter encryptParameter) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
    this.list = list;
    this.dataTypes = list.getTsDataTypes();
    this.sortedList = new ArrayList<>();
    this.encryptParameter = encryptParameter;
  }

  public Set<String> getAllMeasurements() {
    return measurementIndexMap.keySet();
  }

  public boolean containsMeasurement(String measurementId) {
    return measurementIndexMap.containsKey(measurementId);
  }

  int getMeasurementIndex(String measurementId) {
    return measurementIndexMap.get(measurementId);
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
    this.list =
        AlignedTVList.newAlignedList(new ArrayList<>(dataTypes), true, preallocatedBlockNum);
    this.dataTypes = list.getTsDataTypes();
  }

  @Override
  public void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    int columnCount = ensureColumnsAndCount(schemaList);
    if (columnCount == list.getTsDataTypes().size() && schemaList.size() == columnCount) {
      // The plan covers every device column, so the plan index of a measurement is also its value
      // slot: the values can be passed through unchanged and only the destination columns are
      // needed. Reordering the caller's array in place would save the destination array too, but
      // it mutates an array owned by the caller for one int[] allocation, and the destination
      // mapping does not have to be sorted anyway. The identity mapping skips even that array.
      if (isIdentityColumnMapping(schemaList, columnCount)) {
        list.putAlignedValue(insertTime, objectValue);
      } else {
        int[] destColumns = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
          destColumns[i] = measurementIndexMap.get(schemaList.get(i).getMeasurementName());
        }
        list.putAlignedValue(insertTime, objectValue, destColumns);
      }
    } else {
      // Some device columns are missing from the plan: compact the values so that the sparse
      // mapping only walks the written columns.
      int[] destColumns = new int[columnCount];
      Object[] sparseValues = new Object[columnCount];
      int position = 0;
      for (int i = 0; i < schemaList.size(); i++) {
        IMeasurementSchema measurementSchema = schemaList.get(i);
        if (measurementSchema == null) {
          continue;
        }
        destColumns[position] = measurementIndexMap.get(measurementSchema.getMeasurementName());
        sparseValues[position] = objectValue[i];
        position++;
      }
      list.putAlignedValue(insertTime, sparseValues, destColumns);
    }
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
    int columnCount = ensureColumnsAndCount(schemaList);
    int[] destColumns = new int[columnCount];
    Object[] sparseValues = new Object[columnCount];
    BitMap[] sparseBitMaps = bitMaps == null ? null : new BitMap[columnCount];
    int position = 0;
    for (int i = 0; i < schemaList.size(); i++) {
      IMeasurementSchema measurementSchema = schemaList.get(i);
      if (measurementSchema == null) {
        continue;
      }
      destColumns[position] = measurementIndexMap.get(measurementSchema.getMeasurementName());
      sparseValues[position] = valueList[i];
      if (sparseBitMaps != null) {
        sparseBitMaps[position] = bitMaps[i];
      }
      position++;
    }
    list.putAlignedValues(times, sparseValues, sparseBitMaps, destColumns, start, end, results);
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
  }

  /**
   * Register missing measurements in insertion order and return the number of columns written by
   * this insert plan.
   *
   * <p>Only the written columns are materialized by the caller, so a single-measurement write to a
   * wide aligned device no longer allocates device-wide value and bitmap arrays.
   *
   * @param schemaListInInsertPlan Contains all existed schema in InsertPlan. If some timeseries
   *     have been deleted, there will be null in its slot.
   */
  private int ensureColumnsAndCount(List<IMeasurementSchema> schemaListInInsertPlan) {
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      IMeasurementSchema measurementSchema = schemaListInInsertPlan.get(i);
      if (measurementSchema != null
          && !measurementIndexMap.containsKey(measurementSchema.getMeasurementName())) {
        // Only the first missing measurement triggers a batch extension.
        extendColumnsInInsertPlan(schemaListInInsertPlan, i);
        break;
      }
    }
    int columnCount = 0;
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      if (schemaListInInsertPlan.get(i) != null) {
        columnCount++;
      }
    }
    return columnCount;
  }

  /**
   * Whether every column of the insert plan maps to the device column with the same position. Such
   * a plan can be written through the dense path without building a column mapping.
   */
  private boolean isIdentityColumnMapping(
      List<IMeasurementSchema> schemaListInInsertPlan, int columnCount) {
    if (schemaListInInsertPlan.size() != columnCount) {
      return false;
    }
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      IMeasurementSchema measurementSchema = schemaListInInsertPlan.get(i);
      if (measurementSchema == null
          || measurementIndexMap.get(measurementSchema.getMeasurementName()) != i) {
        return false;
      }
    }
    return true;
  }

  private void extendColumnsInInsertPlan(
      List<IMeasurementSchema> schemaListInInsertPlan, int startIndex) {
    List<TSDataType> newDataTypes = new ArrayList<>();
    for (int i = startIndex; i < schemaListInInsertPlan.size(); i++) {
      IMeasurementSchema measurementSchema = schemaListInInsertPlan.get(i);
      if (measurementSchema != null
          && !measurementIndexMap.containsKey(measurementSchema.getMeasurementName())) {
        measurementIndexMap.put(measurementSchema.getMeasurementName(), schemaList.size());
        schemaList.add(measurementSchema);
        newDataTypes.add(measurementSchema.getType());
      }
    }
    list.extendColumns(newDataTypes);
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
  public long count(boolean ignoreAllNullRows) {
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
  public long getMaxTime(boolean ignoreAllNullRows) {
    if (isEmpty(ignoreAllNullRows)) {
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
    return new AlignedChunkWriterImpl(getActiveSchemaList(), encryptParameter);
  }

  private List<IMeasurementSchema> getActiveSchemaList() {
    if (measurementIndexMap.size() == schemaList.size()) {
      return schemaList;
    }
    List<IMeasurementSchema> activeSchemaList = new ArrayList<>(measurementIndexMap.size());
    for (int i = 0; i < schemaList.size(); i++) {
      IMeasurementSchema schema = schemaList.get(i);
      Integer activeColumnIndex = measurementIndexMap.get(schema.getMeasurementName());
      if (activeColumnIndex != null && activeColumnIndex == i) {
        activeSchemaList.add(schema);
      }
    }
    return activeSchemaList;
  }

  private List<TSDataType> getDataTypes(List<IMeasurementSchema> schemaList) {
    if (schemaList == this.schemaList) {
      return dataTypes;
    }
    List<TSDataType> activeDataTypes = new ArrayList<>(schemaList.size());
    for (IMeasurementSchema schema : schemaList) {
      activeDataTypes.add(schema.getType());
    }
    return activeDataTypes;
  }

  @SuppressWarnings({"squid:S6541", "squid:S3776"})
  public void encodeWorkingAlignedTVList(
      BlockingQueue<Object> ioTaskQueue,
      long maxNumberOfPointsInChunk,
      int maxNumberOfPointsInPage,
      boolean ignoreAllNullRows) {
    List<IMeasurementSchema> activeSchemaList = getActiveSchemaList();
    if (activeSchemaList.isEmpty() && ignoreAllNullRows) {
      return;
    }
    // Flush reads the raw value/bitmap containers, so the null tails of the open block must be
    // materialized before encoding.
    ((AlignedTVList) workingListForFlush).materializeOpenBlockForRead();

    BitMap allValueColDeletedMap;
    AlignedTVList alignedWorkingListForFlush = (AlignedTVList) workingListForFlush;

    allValueColDeletedMap =
        ignoreAllNullRows ? alignedWorkingListForFlush.getAllValueColDeletedMap() : null;

    boolean hasDuplicateTimestamp = false;

    List<List<Integer>> chunkRange = new ArrayList<>();
    // Eg. chunkRange: ((0,9,10,12),(13,15)) means this TVList contains 2 chunks,
    // 1st chunk contains 2 pages, 2nd chunk contains 1 page.
    List<Integer> pageRange = new ArrayList<>();

    int pointNumInPage = 0;
    int pointNumInChunk = 0;

    for (int sortedRowIndex = 0;
        sortedRowIndex < alignedWorkingListForFlush.rowCount();
        sortedRowIndex++) {
      long time = alignedWorkingListForFlush.getTime(sortedRowIndex);
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
      while (nextRowIndex < alignedWorkingListForFlush.rowCount()
          && ((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(
                      alignedWorkingListForFlush.getValueIndex(nextRowIndex)))
              || alignedWorkingListForFlush.isTimeDeleted(nextRowIndex))) {
        nextRowIndex++;
      }
      if (nextRowIndex != alignedWorkingListForFlush.rowCount()
          && time == alignedWorkingListForFlush.getTime(nextRowIndex)) {
        // This flag only selects the dense fast path; duplicates are merged during encoding.
        hasDuplicateTimestamp = true;
      }
      sortedRowIndex = nextRowIndex - 1;
    }

    if (pointNumInPage != 0) {
      pageRange.add(alignedWorkingListForFlush.rowCount() - 1);
    }
    if (pointNumInChunk != 0) {
      chunkRange.add(pageRange);
    }

    handleEncoding(
        ioTaskQueue,
        chunkRange,
        hasDuplicateTimestamp,
        allValueColDeletedMap,
        maxNumberOfPointsInPage,
        activeSchemaList);
  }

  private void handleEncoding(
      BlockingQueue<Object> ioTaskQueue,
      List<List<Integer>> chunkRange,
      boolean hasDuplicateTimestamp,
      BitMap allValueColDeletedMap,
      int maxNumberOfPointsInPage,
      List<IMeasurementSchema> activeSchemaList) {
    AlignedTVList alignedWorkingListForFlush = (AlignedTVList) workingListForFlush;
    // Flushing memtables are immutable, so this identity remains stable during encoding.
    boolean hasTimeDeleted = alignedWorkingListForFlush.getTimeColDeletedMap() != null;
    if (!hasDuplicateTimestamp
        && allValueColDeletedMap == null
        && !hasTimeDeleted
        && activeSchemaList == schemaList) {
      handleEncodingWithoutDeletedMeasurementsFastPath(ioTaskQueue, chunkRange);
      return;
    }
    encodePages(
        ioTaskQueue, chunkRange, allValueColDeletedMap, maxNumberOfPointsInPage, activeSchemaList);
  }

  /** Encode pages in one row-major pass, merging duplicate timestamps into groups. */
  private void encodePages(
      BlockingQueue<Object> ioTaskQueue,
      List<List<Integer>> chunkRange,
      BitMap allValueColDeletedMap,
      int maxNumberOfPointsInPage,
      List<IMeasurementSchema> activeSchemaList) {
    AlignedTVList alignedWorkingListForFlush = (AlignedTVList) workingListForFlush;
    int columnCount = activeSchemaList.size();
    List<Integer> columnIndexList = buildColumnIndexList(activeSchemaList);
    // Unbox the column mapping once instead of on every row/column access.
    int[] columnIndexes = new int[columnCount];
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnIndexes[columnIndex] = columnIndexList.get(columnIndex);
    }
    TypeServices.AlignedTVListChunkWriter[] valueWriters =
        new TypeServices.AlignedTVListChunkWriter[columnCount];
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      valueWriters[columnIndex] =
          TypeServices.StorageEngine.ALIGNED_TV_LIST_CHUNK_WRITER_SERVICE.call(
              Type.fromTsDataType(activeSchemaList.get(columnIndex).getType()));
    }
    int[] lastValidPointIndex = new int[columnCount];
    Arrays.fill(lastValidPointIndex, -1);
    // write() only reads [0, pointsInPage), so one array can back every page of this encode.
    long[] times =
        new long[Math.min(maxNumberOfPointsInPage, alignedWorkingListForFlush.rowCount())];

    boolean hasPendingGroup = false;
    long pendingTime = 0;
    int pendingLastRowValueIndex = -1;

    for (int chunkIndex = 0; chunkIndex < chunkRange.size(); chunkIndex++) {
      List<Integer> pageRange = chunkRange.get(chunkIndex);
      AlignedChunkWriterImpl alignedChunkWriter =
          new AlignedChunkWriterImpl(activeSchemaList, encryptParameter);
      ValueChunkWriter[] valueChunkWriters = new ValueChunkWriter[columnCount];
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        valueChunkWriters[columnIndex] = alignedChunkWriter.getValueChunkWriterByIndex(columnIndex);
      }
      int pageCount = pageRange.size() / 2;
      for (int pageNum = 0; pageNum < pageCount; pageNum++) {
        int pageStart = pageRange.get(pageNum * 2);
        int pageEnd = pageRange.get(pageNum * 2 + 1);
        int pointsInPage = 0;

        for (int sortedRowIndex = pageStart; sortedRowIndex <= pageEnd; sortedRowIndex++) {
          int valueIndex = alignedWorkingListForFlush.getValueIndex(sortedRowIndex);
          // Skip empty and deleted rows.
          if ((allValueColDeletedMap != null && allValueColDeletedMap.isMarked(valueIndex))
              || alignedWorkingListForFlush.isTimeDeleted(valueIndex, false)) {
            continue;
          }
          long time = alignedWorkingListForFlush.getTime(sortedRowIndex);
          if (hasPendingGroup && time != pendingTime) {
            pointsInPage =
                appendPendingGroup(
                    alignedWorkingListForFlush,
                    pendingTime,
                    pendingLastRowValueIndex,
                    lastValidPointIndex,
                    columnIndexes,
                    valueWriters,
                    valueChunkWriters,
                    times,
                    pointsInPage);
            hasPendingGroup = false;
          }
          if (!hasPendingGroup) {
            hasPendingGroup = true;
            pendingTime = time;
          }
          pendingLastRowValueIndex = valueIndex;
          for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int tvListColumnIndex = columnIndexes[columnIndex];
            if (tvListColumnIndex >= 0
                && !alignedWorkingListForFlush.isNullValue(valueIndex, tvListColumnIndex)) {
              lastValidPointIndex[columnIndex] = valueIndex;
            }
          }
        }

        // Page ranges contain only non-deleted rows, so the next page's first row is exactly the
        // next non-deleted row: the pending group continues iff that row repeats pendingTime.
        int nextPageStart;
        if (pageNum + 1 < pageCount) {
          nextPageStart = pageRange.get((pageNum + 1) * 2);
        } else if (chunkIndex + 1 < chunkRange.size()) {
          nextPageStart = chunkRange.get(chunkIndex + 1).get(0);
        } else {
          nextPageStart = -1;
        }
        if (hasPendingGroup
            && (nextPageStart < 0
                || alignedWorkingListForFlush.getTime(nextPageStart) != pendingTime)) {
          pointsInPage =
              appendPendingGroup(
                  alignedWorkingListForFlush,
                  pendingTime,
                  pendingLastRowValueIndex,
                  lastValidPointIndex,
                  columnIndexes,
                  valueWriters,
                  valueChunkWriters,
                  times,
                  pointsInPage);
          hasPendingGroup = false;
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

  private int appendPendingGroup(
      AlignedTVList list,
      long pendingTime,
      int pendingLastRowValueIndex,
      int[] lastValidPointIndex,
      int[] columnIndexes,
      TypeServices.AlignedTVListChunkWriter[] valueWriters,
      ValueChunkWriter[] valueChunkWriters,
      long[] times,
      int pointsInPage) {
    for (int columnIndex = 0; columnIndex < columnIndexes.length; columnIndex++) {
      int tvListColumnIndex = columnIndexes[columnIndex];
      boolean isNull = tvListColumnIndex < 0 || lastValidPointIndex[columnIndex] < 0;
      int originRowIndex = isNull ? pendingLastRowValueIndex : lastValidPointIndex[columnIndex];
      valueWriters[columnIndex].write(
          valueChunkWriters[columnIndex],
          pendingTime,
          list,
          originRowIndex,
          tvListColumnIndex,
          isNull);
    }
    times[pointsInPage] = pendingTime;
    Arrays.fill(lastValidPointIndex, -1);
    return pointsInPage + 1;
  }

  private void handleEncodingWithoutDeletedMeasurementsFastPath(
      BlockingQueue<Object> ioTaskQueue, List<List<Integer>> chunkRange) {
    AlignedTVList alignedWorkingListForFlush = (AlignedTVList) workingListForFlush;
    List<TSDataType> dataTypes = alignedWorkingListForFlush.getTsDataTypes();
    // Sorting rearranges timestamps in place, so sorted row offsets map directly to these arrays.
    List<long[]> timestamps = alignedWorkingListForFlush.getTimestamps();
    List<List<Object>> values = alignedWorkingListForFlush.getValues();
    List<List<BitMap>> bitMaps = alignedWorkingListForFlush.getBitMaps();
    BitMap segmentMovedMap = alignedWorkingListForFlush.getSegmentMovedMap();
    boolean[] nulls = new boolean[ARRAY_SIZE];
    for (List<Integer> pageRange : chunkRange) {
      AlignedChunkWriterImpl alignedChunkWriter =
          new AlignedChunkWriterImpl(schemaList, encryptParameter);
      for (int pageNum = 0; pageNum < pageRange.size() / 2; pageNum++) {
        int pageStart = pageRange.get(pageNum * 2);
        int pageEnd = pageRange.get(pageNum * 2 + 1);
        for (int segmentStart = pageStart; segmentStart <= pageEnd; ) {
          int arrayIndex = segmentStart / ARRAY_SIZE;
          int arrayOffset = segmentStart % ARRAY_SIZE;
          int segmentEnd = Math.min(pageEnd, (arrayIndex + 1) * ARRAY_SIZE - 1);
          int pointsInSegment = segmentEnd - segmentStart + 1;
          long[] timestampArray = timestamps.get(arrayIndex);
          boolean segmentMoved = segmentMovedMap != null && segmentMovedMap.isMarked(arrayIndex);
          for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
            TSDataType tsDataType = dataTypes.get(columnIndex);
            ValueChunkWriter valueChunkWriter =
                alignedChunkWriter.getValueChunkWriterByIndex(columnIndex);
            List<Object> columnValues = values.get(columnIndex);
            Object valueArray = columnValues == null ? null : columnValues.get(arrayIndex);
            if (columnValues == null || (!segmentMoved && valueArray == null)) {
              // An unmoved, unmaterialized segment is all null. A moved segment can reference
              // non-null values in another array, unless the entire column is absent.
              valueChunkWriter.getPageWriter().writeNull(pointsInSegment);
            } else if (!segmentMoved) {
              BitMap columnBitMap = null;
              if (bitMaps != null && bitMaps.get(columnIndex) != null) {
                columnBitMap = bitMaps.get(columnIndex).get(arrayIndex);
              }
              if (columnBitMap == null) {
                writeValuesFromArray(
                    valueChunkWriter,
                    tsDataType,
                    timestampArray,
                    valueArray,
                    nulls,
                    arrayOffset,
                    pointsInSegment);
              } else {
                writeValuesFromArray(
                    valueChunkWriter,
                    tsDataType,
                    timestampArray,
                    valueArray,
                    columnBitMap,
                    arrayOffset,
                    pointsInSegment);
              }
            } else {
              for (int sortedRowIndex = segmentStart;
                  sortedRowIndex <= segmentEnd;
                  sortedRowIndex++) {
                int valueIndex = alignedWorkingListForFlush.getValueIndex(sortedRowIndex);
                int valueArrayIndex = valueIndex / ARRAY_SIZE;
                int valueElementIndex = valueIndex % ARRAY_SIZE;
                Object sourceValueArray = columnValues.get(valueArrayIndex);
                boolean isNull =
                    sourceValueArray == null
                        || alignedWorkingListForFlush.isNullValue(valueIndex, columnIndex);
                writeValueFromArray(
                    valueChunkWriter,
                    tsDataType,
                    timestampArray[arrayOffset + sortedRowIndex - segmentStart],
                    sourceValueArray,
                    valueElementIndex,
                    isNull);
              }
            }
          }
          alignedChunkWriter.write(timestampArray, pointsInSegment, arrayOffset);
          segmentStart = segmentEnd + 1;
        }
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

  private void writeValueFromArray(
      ValueChunkWriter valueChunkWriter,
      TSDataType dataType,
      long time,
      Object valueArray,
      int elementIndex,
      boolean isNull) {
    TypeServices.StorageEngine.VALUE_CHUNK_ARRAY_WRITER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .write(valueChunkWriter, time, valueArray, elementIndex, isNull);
  }

  private void writeValuesFromArray(
      ValueChunkWriter valueChunkWriter,
      TSDataType dataType,
      long[] timestamps,
      Object valueArray,
      boolean[] nulls,
      int arrayOffset,
      int pointsInSegment) {
    TypeServices.StorageEngine.VALUE_CHUNK_ARRAY_BATCH_WRITER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .write(valueChunkWriter, timestamps, valueArray, nulls, arrayOffset, pointsInSegment);
  }

  private void writeValuesFromArray(
      ValueChunkWriter valueChunkWriter,
      TSDataType dataType,
      long[] timestamps,
      Object valueArray,
      BitMap bitMap,
      int arrayOffset,
      int pointsInSegment) {
    TypeServices.StorageEngine.VALUE_CHUNK_ARRAY_BITMAP_WRITER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .write(valueChunkWriter, timestamps, valueArray, bitMap, arrayOffset, pointsInSegment);
  }

  @Override
  public void encode(
      BlockingQueue<Object> ioTaskQueue,
      BatchEncodeInfo encodeInfo,
      long[] times,
      boolean ignoreAllNullRows) {
    // The cap is derived from this memchunk's largest column. BatchEncodeInfo is shared by all
    // devices of a flush task, so keep the narrowed cap local to this encode call and restore the
    // original value afterwards; otherwise one wide TEXT column would shrink the chunk size of
    // every following device in the same flush.
    long originalMaxNumberOfPointsInChunk = encodeInfo.maxNumberOfPointsInChunk;
    try {
      encodeInfo.maxNumberOfPointsInChunk =
          Math.min(
              originalMaxNumberOfPointsInChunk,
              (encodeInfo.targetChunkSize / getAvgPointSizeOfLargestColumn()));
      encodeWithComputedChunkCap(ioTaskQueue, encodeInfo, times, ignoreAllNullRows);
    } finally {
      encodeInfo.maxNumberOfPointsInChunk = originalMaxNumberOfPointsInChunk;
    }
  }

  private void encodeWithComputedChunkCap(
      BlockingQueue<Object> ioTaskQueue,
      BatchEncodeInfo encodeInfo,
      long[] times,
      boolean ignoreAllNullRows) {
    if (TVLIST_SORT_THRESHOLD == 0) {
      encodeWorkingAlignedTVList(
          ioTaskQueue,
          encodeInfo.maxNumberOfPointsInChunk,
          encodeInfo.maxNumberOfPointsInPage,
          ignoreAllNullRows);
      return;
    }

    List<IMeasurementSchema> activeSchemaList = getActiveSchemaList();
    if (activeSchemaList.isEmpty() && ignoreAllNullRows) {
      return;
    }
    // Flush reads the raw value/bitmap containers, so the null tails of the open block must be
    // materialized before encoding.
    ((AlignedTVList) workingListForFlush).materializeOpenBlockForRead();

    AlignedChunkWriterImpl alignedChunkWriter =
        new AlignedChunkWriterImpl(activeSchemaList, encryptParameter);

    // create MergeSortAlignedTVListIterator.
    List<AlignedTVList> alignedTvLists = new ArrayList<>(sortedList);
    alignedTvLists.add((AlignedTVList) workingListForFlush);
    List<Integer> columnIndexList = buildColumnIndexList(activeSchemaList);
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(
            getDataTypes(activeSchemaList),
            columnIndexList,
            alignedTvLists,
            ignoreAllNullRows,
            encodeInfo.maxNumberOfPointsInPage,
            null);

    while (timeValuePairIterator.hasNextBatch()) {
      timeValuePairIterator.encodeBatch(alignedChunkWriter, encodeInfo, times);
      if (encodeInfo.pointNumInPage >= encodeInfo.maxNumberOfPointsInPage) {
        alignedChunkWriter.write(times, encodeInfo.pointNumInPage, 0);
        encodeInfo.pointNumInPage = 0;
      }

      if (encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk) {
        flushPendingPageAndEnqueueChunk(ioTaskQueue, alignedChunkWriter, encodeInfo, times);
        alignedChunkWriter = new AlignedChunkWriterImpl(activeSchemaList, encryptParameter);
      }
    }

    // last batch of points
    if (encodeInfo.pointNumInChunk > 0) {
      flushPendingPageAndEnqueueChunk(ioTaskQueue, alignedChunkWriter, encodeInfo, times);
    }
  }

  /**
   * Flush the pending page, if any, and seal the chunk before enqueueing it for IO.
   *
   * <p>{@code encodeBatch} may return with a partially filled page: when the chunk cap is reached
   * in the middle of a page, the values of that page have already been written into the value chunk
   * writers, while the corresponding time page has not been written yet. Sealing the chunk without
   * flushing the partial page first would make the time chunk contain one page less than the value
   * chunks, so the rows of that page could never be read and the chunk statistics would diverge.
   */
  private void flushPendingPageAndEnqueueChunk(
      BlockingQueue<Object> ioTaskQueue,
      AlignedChunkWriterImpl alignedChunkWriter,
      BatchEncodeInfo encodeInfo,
      long[] times) {
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
  public long getLastPoint(boolean ignoreAllNullRows) {
    if (rowCount() == 0) {
      return Long.MIN_VALUE;
    }
    return getMaxTime(ignoreAllNullRows);
  }

  @Override
  public boolean isEmpty(boolean ignoreAllNullRows) {
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
      size += getSerializedSchemaSize(schema);
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
      buffer.put(serializeSchemaToWALBytes(schema));
    }
    buffer.putInt(sortedList.size());
    for (AlignedTVList alignedTvList : sortedList) {
      alignedTvList.serializeToWAL(buffer);
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
    int sortedListSize = stream.readInt();
    List<AlignedTVList> sortedList = new ArrayList<>(sortedListSize);
    for (int i = 0; i < sortedListSize; i++) {
      AlignedTVList tvList = AlignedTVList.deserialize(stream);
      sortedList.add(tvList);
    }
    AlignedTVList list = AlignedTVList.deserialize(stream);
    AlignedWritableMemChunk chunk = new AlignedWritableMemChunk(schemaList, list);
    chunk.sortedList = sortedList;
    return chunk;
  }

  public static AlignedWritableMemChunk deserializeSingleTVListMemChunks(DataInputStream stream)
      throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList list = AlignedTVList.deserialize(stream);
    return new AlignedWritableMemChunk(schemaList, list);
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
