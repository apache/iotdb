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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.TsBlockUtil;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.TVLIST_SORT_ALGORITHM;
import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;
import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

public abstract class AlignedTVList extends TVList {

  // Data types of this aligned tvList
  protected List<TSDataType> dataTypes;

  // Record total memory size of binary column
  protected long[] memoryBinaryChunkSize;

  // Data type list -> list of TVList, add 1 when expanded -> primitive array of basic type
  // Index relation: columnIndex(dataTypeIndex) -> arrayIndex -> elementIndex
  protected List<List<Object>> values;

  // Data type list -> list of BitMap, add 1 when expanded -> BitMap(maybe null), marked means the
  // Value is null
  // Index relation: columnIndex(dataTypeIndex) -> arrayIndex -> elementIndex
  protected List<List<BitMap>> bitMaps;

  // not null when constructed by queries for tree model
  BitMap allValueColDeletedMap;
  // constructed after deletion
  BitMap timeColDeletedMap;

  protected int timeDeletedCnt = 0;

  private final AlignedTVList outer = this;

  AlignedTVList(List<TSDataType> types) {
    super();
    dataTypes = types;
    memoryBinaryChunkSize = new long[dataTypes.size()];

    values = new ArrayList<>(types.size());
    for (int i = 0; i < types.size(); i++) {
      values.add(new ArrayList<>(getDefaultArrayNum()));
    }
  }

  public static AlignedTVList newAlignedList(List<TSDataType> dataTypes) {
    switch (TVLIST_SORT_ALGORITHM) {
      case QUICK:
        return new QuickAlignedTVList(dataTypes);
      case BACKWARD:
        return new BackAlignedTVList(dataTypes);
      default:
        return new TimAlignedTVList(dataTypes);
    }
  }

  private List<Object> convertToType(TSDataType to, TSDataType from, List<Object> originalValues) {
    if (!to.isCompatible(from)) {
      return null;
    }
    return originalValues.stream().map(o -> to.castFromArray(from, o)).collect(Collectors.toList());
  }

  @Override
  public TVList getTvListByColumnIndex(
      List<Integer> columnIndexList, List<TSDataType> dataTypeList, boolean ignoreAllNullRows) {
    List<List<Object>> values = new ArrayList<>();
    List<List<BitMap>> bitMaps = null;
    for (int i = 0; i < columnIndexList.size(); i++) {
      // columnIndex == -1 means querying a non-exist column, add null column here
      Integer columnIndex = columnIndexList.get(i);
      if (columnIndex == -1) {
        values.add(null);
      } else {
        List<Object> column = this.values.get(columnIndex);
        if (dataTypeList.get(i) == this.dataTypes.get(columnIndex)) {
          values.add(column);
        } else {
          values.add(convertToType(dataTypeList.get(i), this.dataTypes.get(columnIndex), column));
        }

        if (this.bitMaps != null && this.bitMaps.get(columnIndex) != null) {
          if (bitMaps == null) {
            bitMaps = new ArrayList<>(columnIndexList.size());
            for (int j = 0; j < columnIndexList.size(); j++) {
              bitMaps.add(null);
            }
          }
          bitMaps.set(i, this.bitMaps.get(columnIndex));
        }
      }
    }
    AlignedTVList alignedTvList = AlignedTVList.newAlignedList(new ArrayList<>(dataTypeList));
    alignedTvList.timestamps = this.timestamps;
    alignedTvList.indices = this.indices;
    alignedTvList.values = values;
    alignedTvList.bitMaps = bitMaps;
    alignedTvList.rowCount = this.rowCount;
    // for table model, we won't discard any row even if all value columns are null
    alignedTvList.allValueColDeletedMap = ignoreAllNullRows ? getAllValueColDeletedMap() : null;
    alignedTvList.timeColDeletedMap = this.timeColDeletedMap;
    alignedTvList.timeDeletedCnt = this.timeDeletedCnt;

    return alignedTvList;
  }

  @Override
  public synchronized AlignedTVList clone() {
    AlignedTVList cloneList = AlignedTVList.newAlignedList(new ArrayList<>(dataTypes));
    cloneAs(cloneList);
    cloneList.timeDeletedCnt = this.timeDeletedCnt;
    System.arraycopy(
        memoryBinaryChunkSize, 0, cloneList.memoryBinaryChunkSize, 0, dataTypes.size());
    for (int i = 0; i < values.size(); i++) {
      // Clone value
      List<Object> columnValues = values.get(i);
      for (Object valueArray : columnValues) {
        cloneList.values.get(i).add(cloneValue(dataTypes.get(i), valueArray));
      }
      // Clone bitmap in columnIndex
      if (bitMaps != null && bitMaps.get(i) != null) {
        List<BitMap> columnBitMaps = bitMaps.get(i);
        if (cloneList.bitMaps == null) {
          cloneList.bitMaps = new ArrayList<>(dataTypes.size());
          for (int j = 0; j < dataTypes.size(); j++) {
            cloneList.bitMaps.add(null);
          }
        }
        if (cloneList.bitMaps.get(i) == null) {
          List<BitMap> cloneColumnBitMaps = new ArrayList<>(columnBitMaps.size());
          for (BitMap bitMap : columnBitMaps) {
            cloneColumnBitMaps.add(bitMap == null ? null : bitMap.clone());
          }
          cloneList.bitMaps.set(i, cloneColumnBitMaps);
        }
      }
    }
    cloneList.timeColDeletedMap = timeColDeletedMap == null ? null : timeColDeletedMap.clone();
    return cloneList;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public synchronized void putAlignedValue(long timestamp, Object[] value) {
    checkExpansion();
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;
    maxTime = Math.max(maxTime, timestamp);
    minTime = Math.min(minTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    for (int i = 0; i < values.size(); i++) {
      Object columnValue = value[i];
      List<Object> columnValues = values.get(i);
      if (columnValue == null) {
        markNullValue(i, arrayIndex, elementIndex);
      }
      switch (dataTypes.get(i)) {
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          ((Binary[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (Binary) columnValue : Binary.EMPTY_VALUE;
          memoryBinaryChunkSize[i] +=
              columnValue != null
                  ? getBinarySize((Binary) columnValue)
                  : getBinarySize(Binary.EMPTY_VALUE);
          break;
        case FLOAT:
          ((float[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (float) columnValue : Float.MIN_VALUE;
          break;
        case INT32:
        case DATE:
          ((int[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (int) columnValue : Integer.MIN_VALUE;
          break;
        case INT64:
        case TIMESTAMP:
          ((long[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (long) columnValue : Long.MIN_VALUE;
          break;
        case DOUBLE:
          ((double[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (double) columnValue : Double.MIN_VALUE;
          break;
        case BOOLEAN:
          ((boolean[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null && (boolean) columnValue;
          break;
        default:
          break;
      }
    }
    if (indices != null) {
      indices.get(arrayIndex)[elementIndex] = rowCount;
    }
    rowCount++;
    if (sorted) {
      if (rowCount > 1 && timestamp < getTime(rowCount - 2)) {
        sorted = false;
      } else {
        seqRowCount++;
      }
    }
  }

  @Override
  public Object getAlignedValue(int index) {
    return getAlignedValueForQuery(index, null, null);
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(
        getTime(index), (TsPrimitiveType) getAlignedValueForQuery(index, null, null));
  }

  private Object getAlignedValueForQuery(
      int index, Integer floatPrecision, List<TSEncoding> encodingList) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int valueIndex =
        (indices != null) ? indices.get(index / ARRAY_SIZE)[index % ARRAY_SIZE] : index;
    return getAlignedValueByValueIndex(valueIndex, null, floatPrecision, encodingList);
  }

  @SuppressWarnings("java:S6541")
  private TsPrimitiveType getAlignedValueByValueIndex(
      int valueIndex,
      int[] validIndexesForTimeDuplicatedRows,
      Integer floatPrecision,
      List<TSEncoding> encodingList) {
    if (valueIndex >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    TsPrimitiveType[] vector = new TsPrimitiveType[values.size()];
    for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
      List<Object> columnValues = values.get(columnIndex);
      int validValueIndex;
      if (validIndexesForTimeDuplicatedRows != null) {
        validValueIndex = validIndexesForTimeDuplicatedRows[columnIndex];
      } else {
        validValueIndex = valueIndex;
      }
      int arrayIndex = validValueIndex / ARRAY_SIZE;
      int elementIndex = validValueIndex % ARRAY_SIZE;
      if (columnValues == null || isNullValue(validValueIndex, columnIndex)) {
        continue;
      }
      switch (dataTypes.get(columnIndex)) {
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          Binary valueT = ((Binary[]) columnValues.get(arrayIndex))[elementIndex];
          vector[columnIndex] = TsPrimitiveType.getByType(TSDataType.TEXT, valueT);
          break;
        case FLOAT:
          float valueF = ((float[]) columnValues.get(arrayIndex))[elementIndex];
          if (floatPrecision != null
              && encodingList != null
              && !Float.isNaN(valueF)
              && (encodingList.get(columnIndex) == TSEncoding.RLE
                  || encodingList.get(columnIndex) == TSEncoding.TS_2DIFF)) {
            valueF = MathUtils.roundWithGivenPrecision(valueF, floatPrecision);
          }
          vector[columnIndex] = TsPrimitiveType.getByType(TSDataType.FLOAT, valueF);
          break;
        case INT32:
        case DATE:
          int valueI = ((int[]) columnValues.get(arrayIndex))[elementIndex];
          vector[columnIndex] = TsPrimitiveType.getByType(TSDataType.INT32, valueI);
          break;
        case INT64:
        case TIMESTAMP:
          long valueL = ((long[]) columnValues.get(arrayIndex))[elementIndex];
          vector[columnIndex] = TsPrimitiveType.getByType(TSDataType.INT64, valueL);
          break;
        case DOUBLE:
          double valueD = ((double[]) columnValues.get(arrayIndex))[elementIndex];
          if (floatPrecision != null
              && encodingList != null
              && !Double.isNaN(valueD)
              && (encodingList.get(columnIndex) == TSEncoding.RLE
                  || encodingList.get(columnIndex) == TSEncoding.TS_2DIFF)) {
            valueD = MathUtils.roundWithGivenPrecision(valueD, floatPrecision);
          }
          vector[columnIndex] = TsPrimitiveType.getByType(TSDataType.DOUBLE, valueD);
          break;
        case BOOLEAN:
          boolean valueB = ((boolean[]) columnValues.get(arrayIndex))[elementIndex];
          vector[columnIndex] = TsPrimitiveType.getByType(TSDataType.BOOLEAN, valueB);
          break;
        default:
          throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
      }
    }
    return TsPrimitiveType.getByType(TSDataType.VECTOR, vector);
  }

  public void extendColumn(TSDataType dataType) {
    if (bitMaps == null) {
      List<List<BitMap>> localBitMaps = new ArrayList<>(values.size());
      for (int i = 0; i < values.size(); i++) {
        localBitMaps.add(null);
      }
      bitMaps = localBitMaps;
    }
    List<Object> columnValue = new ArrayList<>(timestamps.size());
    List<BitMap> columnBitMaps = new ArrayList<>(timestamps.size());
    for (int i = 0; i < timestamps.size(); i++) {
      switch (dataType) {
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          columnValue.add(getPrimitiveArraysByType(TSDataType.TEXT));
          break;
        case FLOAT:
          columnValue.add(getPrimitiveArraysByType(TSDataType.FLOAT));
          break;
        case INT32:
        case DATE:
          columnValue.add(getPrimitiveArraysByType(TSDataType.INT32));
          break;
        case INT64:
        case TIMESTAMP:
          columnValue.add(getPrimitiveArraysByType(TSDataType.INT64));
          break;
        case DOUBLE:
          columnValue.add(getPrimitiveArraysByType(TSDataType.DOUBLE));
          break;
        case BOOLEAN:
          columnValue.add(getPrimitiveArraysByType(TSDataType.BOOLEAN));
          break;
        default:
          break;
      }
      BitMap bitMap = new BitMap(ARRAY_SIZE);
      // The following code is for these 2 kinds of scenarios.

      // Eg1: If rowCount=5 and ARRAY_SIZE=2, we need to supply 3 bitmaps for the extending column.
      // The first 2 bitmaps should mark all bits to represent 4 nulls and the 3rd bitmap should
      // mark
      // the 1st bit to represent 1 null value.

      // Eg2: If rowCount=4 and ARRAY_SIZE=2, we need to supply 2 bitmaps for the extending column.
      // These 2 bitmaps should mark all bits to represent 4 nulls.
      if (i == timestamps.size() - 1 && rowCount % ARRAY_SIZE != 0) {
        bitMap.markRange(0, rowCount % ARRAY_SIZE);
      } else {
        bitMap.markAll();
      }
      columnBitMaps.add(bitMap);
    }
    this.bitMaps.add(columnBitMaps);
    this.values.add(columnValue);
    this.dataTypes.add(dataType);

    long[] tmpValueChunkRawSize = memoryBinaryChunkSize;
    memoryBinaryChunkSize = new long[dataTypes.size()];
    System.arraycopy(
        tmpValueChunkRawSize, 0, memoryBinaryChunkSize, 0, tmpValueChunkRawSize.length);
  }

  private Object getObjectByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    switch (dataTypes.get(columnIndex)) {
      case INT32:
      case DATE:
        return ((int[]) columnValues.get(arrayIndex))[elementIndex];
      case INT64:
      case TIMESTAMP:
        return ((long[]) columnValues.get(arrayIndex))[elementIndex];
      case FLOAT:
        return ((float[]) columnValues.get(arrayIndex))[elementIndex];
      case DOUBLE:
        return ((double[]) columnValues.get(arrayIndex))[elementIndex];
      case BOOLEAN:
        return ((boolean[]) columnValues.get(arrayIndex))[elementIndex];
      case STRING:
      case BLOB:
      case TEXT:
      case OBJECT:
        return ((Binary[]) columnValues.get(arrayIndex))[elementIndex];
      default:
        throw new IllegalArgumentException(dataTypes.get(columnIndex) + " is not supported");
    }
  }

  /**
   * Get the int value at the given position in AlignedTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return the value at this position in VectorTvList
   */
  public int getIntByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    if (dataTypes.get(columnIndex) == TSDataType.INT32
        || dataTypes.get(columnIndex) == TSDataType.DATE) {
      return ((int[]) columnValues.get(arrayIndex))[elementIndex];
    } else {
      return (int)
          TSDataType.INT32.castFromSingleValue(
              dataTypes.get(columnIndex), getObjectByValueIndex(rowIndex, columnIndex));
    }
  }

  /**
   * Get the long value at the given position in VectorTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return the value at this position in VectorTvList
   */
  public long getLongByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    if (dataTypes.get(columnIndex) == TSDataType.INT64
        || dataTypes.get(columnIndex) == TSDataType.TIMESTAMP) {
      return ((long[]) columnValues.get(arrayIndex))[elementIndex];
    } else {
      return (long)
          TSDataType.INT64.castFromSingleValue(
              dataTypes.get(columnIndex), getObjectByValueIndex(rowIndex, columnIndex));
    }
  }

  /**
   * Get the float value at the given position in VectorTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return the value at this position in VectorTvList
   */
  public float getFloatByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    if (dataTypes.get(columnIndex) == TSDataType.FLOAT) {
      return ((float[]) columnValues.get(arrayIndex))[elementIndex];
    } else {
      return (float)
          TSDataType.FLOAT.castFromSingleValue(
              dataTypes.get(columnIndex), getObjectByValueIndex(rowIndex, columnIndex));
    }
  }

  /**
   * Get the double value at the given position in VectorTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return the value at this position in VectorTvList
   */
  public double getDoubleByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    if (dataTypes.get(columnIndex) == TSDataType.DOUBLE) {
      return ((double[]) columnValues.get(arrayIndex))[elementIndex];
    } else {
      return (double)
          TSDataType.DOUBLE.castFromSingleValue(
              dataTypes.get(columnIndex), getObjectByValueIndex(rowIndex, columnIndex));
    }
  }

  /**
   * Get the Binary value at the given position in VectorTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return the value at this position in VectorTvList
   */
  public Binary getBinaryByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    if (dataTypes.get(columnIndex) == TSDataType.TEXT
        || dataTypes.get(columnIndex) == TSDataType.BLOB
        || dataTypes.get(columnIndex) == TSDataType.STRING
        || dataTypes.get(columnIndex) == TSDataType.OBJECT) {
      return ((Binary[]) columnValues.get(arrayIndex))[elementIndex];
    } else {
      return (Binary)
          TSDataType.TEXT.castFromSingleValue(
              dataTypes.get(columnIndex), getObjectByValueIndex(rowIndex, columnIndex));
    }
  }

  /**
   * Get the boolean value at the given position in VectorTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return the value at this position in VectorTvList
   */
  public boolean getBooleanByValueIndex(int rowIndex, int columnIndex) {
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(columnIndex);
    if (dataTypes.get(columnIndex) == TSDataType.BOOLEAN) {
      return ((boolean[]) columnValues.get(arrayIndex))[elementIndex];
    } else {
      return (Boolean)
          TSDataType.BOOLEAN.castFromSingleValue(
              dataTypes.get(columnIndex), getObjectByValueIndex(rowIndex, columnIndex));
    }
  }

  /**
   * Get whether value is null at the given position in AlignedTvList.
   *
   * @param unsortedRowIndex value index inside this column
   * @param columnIndex index of the column
   * @return boolean
   */
  public boolean isNullValue(int unsortedRowIndex, int columnIndex) {
    if (unsortedRowIndex >= rowCount) {
      return false;
    }
    if (allValueColDeletedMap != null && allValueColDeletedMap.isMarked(unsortedRowIndex)) {
      return true;
    }

    if (columnIndex < 0 || columnIndex >= values.size() || values.get(columnIndex) == null) {
      return true;
    }
    if (bitMaps == null
        || bitMaps.get(columnIndex) == null
        || bitMaps.get(columnIndex).get(unsortedRowIndex / ARRAY_SIZE) == null) {
      return false;
    }
    int arrayIndex = unsortedRowIndex / ARRAY_SIZE;
    int elementIndex = unsortedRowIndex % ARRAY_SIZE;
    List<BitMap> columnBitMaps = bitMaps.get(columnIndex);
    return columnBitMaps.get(arrayIndex).isMarked(elementIndex);
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public List<TSDataType> getTsDataTypes() {
    return dataTypes;
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = 0;
    for (int i = 0; i < dataTypes.size(); i++) {
      deletedNumber += delete(lowerBound, upperBound, i).left;
    }
    return deletedNumber;
  }

  public int deleteTime(long lowerBound, long upperBound) {
    delete(lowerBound, upperBound);
    int prevDeletedCnt = this.timeDeletedCnt;
    for (int i = 0; i < rowCount; i++) {
      long time = getTime(i);
      if (time >= lowerBound && time <= upperBound) {
        markRowNull(i);
      }
    }
    boolean needUpdateMaxTime = lowerBound <= maxTime && maxTime <= upperBound;
    if (needUpdateMaxTime) {
      updateMaxTime();
    }
    boolean needUpdateMinTime = lowerBound <= minTime && minTime <= upperBound;
    if (needUpdateMinTime) {
      updateMinTime();
    }
    return timeDeletedCnt - prevDeletedCnt;
  }

  private void updateMaxTime() {
    long maxTime = Long.MIN_VALUE;
    for (int i = 0; i < rowCount; i++) {
      if (!isTimeDeleted(i)) {
        maxTime = Math.max(maxTime, getTime(i));
      }
    }
    this.maxTime = maxTime;
  }

  private void updateMinTime() {
    long minTime = Long.MAX_VALUE;
    for (int i = 0; i < rowCount; i++) {
      if (!isTimeDeleted(i)) {
        minTime = Math.min(minTime, getTime(i));
      }
    }
    this.minTime = minTime;
  }

  /**
   * Delete points in a specific column.
   *
   * @param lowerBound deletion lower bound
   * @param upperBound deletion upper bound
   * @param columnIndex column index to be deleted
   * @return Delete info pair. Left: deletedNumber int; right: ifDeleteColumn boolean
   */
  public Pair<Integer, Boolean> delete(long lowerBound, long upperBound, int columnIndex) {
    if (columnIndex >= values.size()) {
      return new Pair<>(0, false);
    }

    int deletedNumber = 0;
    boolean deleteColumn = true;
    for (int i = 0; i < rowCount; i++) {
      long time = getTime(i);
      if (time >= lowerBound && time <= upperBound) {
        int originRowIndex = getValueIndex(i);
        int arrayIndex = originRowIndex / ARRAY_SIZE;
        int elementIndex = originRowIndex % ARRAY_SIZE;
        if (markNullValue(columnIndex, arrayIndex, elementIndex)) {
          deletedNumber++;
        }
      } else {
        deleteColumn = false;
      }
    }
    return new Pair<>(deletedNumber, deleteColumn);
  }

  public void deleteColumn(int columnIndex) {
    if (bitMaps == null) {
      List<List<BitMap>> localBitMaps = new ArrayList<>(dataTypes.size());
      for (int j = 0; j < dataTypes.size(); j++) {
        localBitMaps.add(null);
      }
      bitMaps = localBitMaps;
    }
    if (bitMaps.get(columnIndex) == null) {
      List<BitMap> columnBitMaps = new ArrayList<>(values.get(columnIndex).size());
      for (int i = 0; i < values.get(columnIndex).size(); i++) {
        columnBitMaps.add(new BitMap(ARRAY_SIZE));
      }
      bitMaps.set(columnIndex, columnBitMaps);
    }
    for (int i = 0; i < bitMaps.get(columnIndex).size(); i++) {
      if (bitMaps.get(columnIndex).get(i) == null) {
        bitMaps.get(columnIndex).set(i, new BitMap(ARRAY_SIZE));
      }
      bitMaps.get(columnIndex).get(i).markAll();
    }
  }

  protected Object cloneValue(TSDataType type, Object value) {
    switch (type) {
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        Binary[] valueT = (Binary[]) value;
        Binary[] cloneT = new Binary[valueT.length];
        System.arraycopy(valueT, 0, cloneT, 0, valueT.length);
        return cloneT;
      case FLOAT:
        float[] valueF = (float[]) value;
        float[] cloneF = new float[valueF.length];
        System.arraycopy(valueF, 0, cloneF, 0, valueF.length);
        return cloneF;
      case INT32:
      case DATE:
        int[] valueI = (int[]) value;
        int[] cloneI = new int[valueI.length];
        System.arraycopy(valueI, 0, cloneI, 0, valueI.length);
        return cloneI;
      case INT64:
      case TIMESTAMP:
        long[] valueL = (long[]) value;
        long[] cloneL = new long[valueL.length];
        System.arraycopy(valueL, 0, cloneL, 0, valueL.length);
        return cloneL;
      case DOUBLE:
        double[] valueD = (double[]) value;
        double[] cloneD = new double[valueD.length];
        System.arraycopy(valueD, 0, cloneD, 0, valueD.length);
        return cloneD;
      case BOOLEAN:
        boolean[] valueB = (boolean[]) value;
        boolean[] cloneB = new boolean[valueB.length];
        System.arraycopy(valueB, 0, cloneB, 0, valueB.length);
        return cloneB;
      default:
        return null;
    }
  }

  @Override
  protected void clearValue() {
    for (int i = 0; i < dataTypes.size(); i++) {
      List<Object> columnValues = values.get(i);
      if (columnValues != null) {
        for (Object dataArray : columnValues) {
          PrimitiveArrayManager.release(dataArray);
        }
        columnValues.clear();
      }
      memoryBinaryChunkSize[i] = 0;
    }
  }

  @Override
  protected void clearBitMap() {
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps != null) {
        List<BitMap> columnBitMaps = bitMaps.get(i);
        if (columnBitMaps != null) {
          columnBitMaps.clear();
        }
      }
    }
  }

  @Override
  protected void expandValues() {
    if (indices != null) {
      indices.add((int[]) getPrimitiveArraysByType(TSDataType.INT32));
    }
    for (int i = 0; i < dataTypes.size(); i++) {
      values.get(i).add(getPrimitiveArraysByType(dataTypes.get(i)));
      if (bitMaps != null && bitMaps.get(i) != null) {
        bitMaps.get(i).add(null);
      }
    }
  }

  /**
   * @return true if the row is marked, false if it is already marked
   */
  private boolean markRowNull(int i) {
    if (timeColDeletedMap == null) {
      timeColDeletedMap = new BitMap(rowCount);
    } else if (timeColDeletedMap.getSize() < rowCount) {
      byte[] prevBytes = timeColDeletedMap.getByteArray();
      byte[] newBytes = new byte[rowCount / 8 + 1];
      System.arraycopy(prevBytes, 0, newBytes, 0, prevBytes.length);
      timeColDeletedMap = new BitMap(rowCount, newBytes);
    }
    // use value index so that sorts will not change the nullability
    if (timeColDeletedMap.isMarked(getValueIndex(i))) {
      return false;
    } else {
      timeColDeletedMap.mark(getValueIndex(i));
      timeDeletedCnt++;
      return true;
    }
  }

  /**
   * Get the valid original row index in a column by a given time duplicated original row index
   * list.
   *
   * @param timeDuplicatedOriginRowIndexList The row index list that the time of all indexes are
   *     same.
   * @param columnIndex The index of a given column.
   * @return The original row index of the latest non-null value, or the first row index if all
   *     values in given columns are null.
   */
  public int getValidRowIndexForTimeDuplicatedRows(
      List<Integer> timeDuplicatedOriginRowIndexList, int columnIndex) {
    int validRowIndex = timeDuplicatedOriginRowIndexList.get(0);
    for (int originRowIndex : timeDuplicatedOriginRowIndexList) {
      if (!isNullValue(originRowIndex, columnIndex)) {
        validRowIndex = originRowIndex;
      }
    }
    return validRowIndex;
  }

  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, List<TSEncoding> encodingList) {
    return new TimeValuePair(
        time, (TsPrimitiveType) getAlignedValueForQuery(index, floatPrecision, encodingList));
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public synchronized void putAlignedValues(
      long[] time, Object[] value, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    checkExpansion();
    int idx = start;

    updateMinMaxTimeAndSorted(time, start, end);

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = rowCount / ARRAY_SIZE;
      int elementIdx = rowCount % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        arrayCopy(value, idx, arrayIdx, elementIdx, inputRemaining);
        for (int i = 0; i < inputRemaining; i++) {
          if (indices != null) {
            indices.get(arrayIdx)[elementIdx + i] = rowCount;
          }
          rowCount++;
        }
        markNullBitmapRange(value, bitMaps, results, idx, elementIdx, inputRemaining, arrayIdx);
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        arrayCopy(value, idx, arrayIdx, elementIdx, internalRemaining);
        for (int i = 0; i < internalRemaining; i++) {
          if (indices != null) {
            indices.get(arrayIdx)[elementIdx + i] = rowCount;
          }
          rowCount++;
        }
        markNullBitmapRange(value, bitMaps, results, idx, elementIdx, internalRemaining, arrayIdx);
        idx += internalRemaining;
        checkExpansion();
      }
    }
  }

  private void markNullBitmapRange(
      Object[] values,
      BitMap[] bitMaps,
      TSStatus[] results,
      int idx,
      int elementIdx,
      int len,
      int arrayIndex) {

    /* 1. Build result-level bitmap (1 = failure row) */
    byte[] resultBitMap =
        (results != null) ? buildResultBitMapBytes(results, idx, elementIdx, len) : null;

    for (int j = 0; j < values.length; j++) {
      /* Fast-path: column is entirely null */
      if (values[j] == null) {
        getBitMap(j, arrayIndex).markRange(elementIdx, len);
        continue;
      }

      /* 2.mask the column bitmap */
      if (bitMaps != null && bitMaps[j] != null) {
        getBitMap(j, arrayIndex).merge(bitMaps[j], idx, elementIdx, len);
      }

      /* 3. Overlay result bitmap (failure rows) */
      if (resultBitMap != null) {
        markNullValue(j, arrayIndex, elementIdx, resultBitMap);
      }
    }
  }

  public static byte[] buildResultBitMapBytes(
      TSStatus[] results, int idx, int elementIdx, int length) {
    int start = elementIdx & 7;
    int totalBits = start + length;
    int size = (totalBits + 7) >> 3;
    BitMap bitmap = new BitMap(size, new byte[size]);

    if (results == null) {
      return bitmap.getByteArray();
    }

    for (int i = 0; i < length; i++) {
      if (results[idx + i] != null
          && results[idx + i].code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        bitmap.mark(start + i);
      }
    }
    return bitmap.getByteArray();
  }

  private void arrayCopy(Object[] value, int idx, int arrayIndex, int elementIndex, int remaining) {
    for (int i = 0; i < values.size(); i++) {
      if (value[i] == null) {
        continue;
      }
      List<Object> columnValues = values.get(i);
      switch (dataTypes.get(i)) {
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          Binary[] arrayT = ((Binary[]) columnValues.get(arrayIndex));
          System.arraycopy(value[i], idx, arrayT, elementIndex, remaining);

          // update raw size of Text chunk
          for (int i1 = 0; i1 < remaining; i1++) {
            memoryBinaryChunkSize[i] +=
                arrayT[elementIndex + i1] != null ? getBinarySize(arrayT[elementIndex + i1]) : 0;
          }
          break;
        case FLOAT:
          float[] arrayF = ((float[]) columnValues.get(arrayIndex));
          System.arraycopy(value[i], idx, arrayF, elementIndex, remaining);
          break;
        case INT32:
        case DATE:
          int[] arrayI = ((int[]) columnValues.get(arrayIndex));
          System.arraycopy(value[i], idx, arrayI, elementIndex, remaining);
          break;
        case INT64:
        case TIMESTAMP:
          long[] arrayL = ((long[]) columnValues.get(arrayIndex));
          System.arraycopy(value[i], idx, arrayL, elementIndex, remaining);
          break;
        case DOUBLE:
          double[] arrayD = ((double[]) columnValues.get(arrayIndex));
          System.arraycopy(value[i], idx, arrayD, elementIndex, remaining);
          break;
        case BOOLEAN:
          boolean[] arrayB = ((boolean[]) columnValues.get(arrayIndex));
          System.arraycopy(value[i], idx, arrayB, elementIndex, remaining);
          break;
        default:
          break;
      }
    }
  }

  private BitMap getBitMap(int columnIndex, int arrayIndex) {
    // init BitMaps if doesn't have
    if (bitMaps == null) {
      List<List<BitMap>> localBitMaps = new ArrayList<>(dataTypes.size());
      for (int i = 0; i < dataTypes.size(); i++) {
        localBitMaps.add(null);
      }
      bitMaps = localBitMaps;
    }

    // if the bitmap in columnIndex is null, init the bitmap of this column from the beginning
    if (bitMaps.get(columnIndex) == null) {
      List<BitMap> columnBitMaps = new ArrayList<>(values.get(columnIndex).size());
      for (int i = 0; i < values.get(columnIndex).size(); i++) {
        columnBitMaps.add(new BitMap(ARRAY_SIZE, new byte[ARRAY_SIZE]));
      }
      bitMaps.set(columnIndex, columnBitMaps);
    }

    // if the bitmap in arrayIndex is null, init the bitmap
    if (bitMaps.get(columnIndex).get(arrayIndex) == null) {
      bitMaps.get(columnIndex).set(arrayIndex, new BitMap(ARRAY_SIZE, new byte[ARRAY_SIZE]));
    }

    return bitMaps.get(columnIndex).get(arrayIndex);
  }

  private void markNullValue(
      int columnIndex, int arrayIndex, int elementIndex, byte[] resultBitMap) {
    byte[] bitMap = getBitMap(columnIndex, arrayIndex).getByteArray();
    int start = elementIndex >>> 3;
    for (byte b : resultBitMap) {
      bitMap[start++] |= b;
    }
  }

  private boolean markNullValue(int columnIndex, int arrayIndex, int elementIndex) {
    // mark the null value in the current bitmap
    BitMap bitMap = getBitMap(columnIndex, arrayIndex);
    if (bitMap.isMarked(elementIndex)) {
      return false;
    } else {
      bitMap.mark(elementIndex);
      return true;
    }
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.VECTOR;
  }

  @Override
  public synchronized long calculateRamSize() {
    return timestamps.size() * alignedTvListArrayMemCost();
  }

  /**
   * Get the single alignedTVList array mem cost by give types.
   *
   * @param types the types in the vector
   * @return AlignedTvListArrayMemSize
   */
  public static long alignedTvListArrayMemCost(
      TSDataType[] types, TsTableColumnCategory[] columnCategories) {

    int measurementColumnNum = 0;
    long size = 0;
    // value array mem size
    for (int i = 0; i < types.length; i++) {
      TSDataType type = types[i];
      if (type != null
          && (columnCategories == null || columnCategories[i] == TsTableColumnCategory.FIELD)) {
        size += (long) ARRAY_SIZE * (long) type.getDataTypeSize();
        measurementColumnNum++;
      }
    }
    // size is 0 when all types are null
    if (size == 0) {
      return size;
    }
    // time array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // array headers mem size
    size += (long) NUM_BYTES_ARRAY_HEADER * (2 + measurementColumnNum);
    // Object references size in ArrayList
    size += (long) NUM_BYTES_OBJECT_REF * (2 + measurementColumnNum);
    return size;
  }

  /**
   * Get the single alignedTVList array mem cost by give types.
   *
   * @return AlignedTvListArrayMemSize
   */
  public long alignedTvListArrayMemCost() {
    long size = 0;
    // value & bitmap array mem size
    for (int column = 0; column < dataTypes.size(); column++) {
      TSDataType type = dataTypes.get(column);
      if (type != null) {
        size += (long) PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
        if (bitMaps != null && bitMaps.get(column) != null) {
          size += (long) PrimitiveArrayManager.ARRAY_SIZE / 8 + 1;
        }
      }
    }
    // size is 0 when all types are null
    if (size == 0) {
      return size;
    }
    // time array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // index array mem size
    size += (indices != null) ? PrimitiveArrayManager.ARRAY_SIZE * 4L : 0;
    // array headers mem size
    size += (long) NUM_BYTES_ARRAY_HEADER * (2 + dataTypes.size());
    // Object references size in ArrayList
    size += (long) NUM_BYTES_OBJECT_REF * (2 + dataTypes.size());
    return size;
  }

  /**
   * Get the single column array mem cost by give type.
   *
   * @param type the type of the value column
   * @return valueListArrayMemCost
   */
  public static long valueListArrayMemCost(TSDataType type) {
    long size = 0;
    // value array mem size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    // bitmap array mem size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE / 8 + 1;
    // array headers mem size
    size += NUM_BYTES_ARRAY_HEADER;
    // Object references size in ArrayList
    size += NUM_BYTES_OBJECT_REF;
    return size;
  }

  /** Build TsBlock by column. */
  @SuppressWarnings("java:S6541")
  public TsBlock buildTsBlock(
      int floatPrecision,
      List<TSEncoding> encodingList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> deletionList,
      boolean ignoreAllNullRows) {
    TsBlockBuilder builder = new TsBlockBuilder(dataTypes);
    // Time column
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
    int validRowCount = 0;

    // duplicated time or deleted time are all invalid, true if we don't need this row
    boolean[] timeInvalidInfo = null;
    int[] deleteCursor = {0};
    // time column
    for (int sortedRowIndex = 0; sortedRowIndex < rowCount; sortedRowIndex++) {
      // skip empty row
      if (allValueColDeletedMap != null
          && allValueColDeletedMap.isMarked(getValueIndex(sortedRowIndex))) {
        continue;
      }
      if (isTimeDeleted(sortedRowIndex)) {
        continue;
      }
      int nextRowIndex = sortedRowIndex + 1;
      while (nextRowIndex < rowCount
          && ((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(getValueIndex(nextRowIndex)))
              || (isTimeDeleted(nextRowIndex)))) {
        nextRowIndex++;
      }
      long timestamp = getTime(sortedRowIndex);
      if ((nextRowIndex == rowCount || timestamp != getTime(nextRowIndex))
          && !isPointDeleted(timestamp, timeColumnDeletion, deleteCursor)) {
        timeBuilder.writeLong(getTime(sortedRowIndex));
        validRowCount++;
      } else {
        if (Objects.isNull(timeInvalidInfo)) {
          timeInvalidInfo = new boolean[rowCount];
        }
        timeInvalidInfo[sortedRowIndex] = true;
      }
      sortedRowIndex = nextRowIndex - 1;
    }

    boolean[] hasAnyNonNullValue = new boolean[validRowCount];
    int columnCount = dataTypes.size();
    int currentWriteRowIndex;
    // value columns
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      deleteCursor = new int[] {0};
      // Pair of Time and Index
      Pair<Long, Integer> lastValidPointIndexForTimeDupCheck = null;
      if (Objects.nonNull(timeInvalidInfo)) {
        lastValidPointIndexForTimeDupCheck = new Pair<>(Long.MIN_VALUE, null);
      }
      ColumnBuilder valueBuilder = builder.getColumnBuilder(columnIndex);
      currentWriteRowIndex = 0;
      for (int sortedRowIndex = 0; sortedRowIndex < rowCount; sortedRowIndex++) {
        // skip empty row
        if ((allValueColDeletedMap != null
                && allValueColDeletedMap.isMarked(getValueIndex(sortedRowIndex)))
            || (isTimeDeleted(sortedRowIndex))) {
          continue;
        }
        // skip time duplicated or totally deleted rows
        if (Objects.nonNull(timeInvalidInfo)) {
          if (!isNullValue(getValueIndex(sortedRowIndex), columnIndex)) {
            lastValidPointIndexForTimeDupCheck.left = getTime(sortedRowIndex);
            lastValidPointIndexForTimeDupCheck.right = getValueIndex(sortedRowIndex);
          }
          if (timeInvalidInfo[sortedRowIndex]) {
            continue;
          }
        }
        // The part of code solves the following problem:
        // Time: 1,2,2,3
        // Value: 1,2,null,null
        // When rowIndex:1, pair(min,null), timeDuplicateInfo:false, write(T:1,V:1)
        // When rowIndex:2, pair(2,2), timeDuplicateInfo:true, skip writing value
        // When rowIndex:3, pair(2,2), timeDuplicateInfo:false, T:2==pair.left:2, write(T:2,V:2)
        // When rowIndex:4, pair(2,2), timeDuplicateInfo:false, T:3!=pair.left:2, write(T:3,V:null)
        int originRowIndex;
        if (Objects.nonNull(lastValidPointIndexForTimeDupCheck)
            && (getTime(sortedRowIndex) == lastValidPointIndexForTimeDupCheck.left)) {
          originRowIndex = lastValidPointIndexForTimeDupCheck.right;
        } else {
          originRowIndex = getValueIndex(sortedRowIndex);
        }
        if (isNullValue(originRowIndex, columnIndex)
            || isPointDeleted(
                getTime(sortedRowIndex),
                Objects.isNull(deletionList) ? null : deletionList.get(columnIndex),
                deleteCursor)) {
          valueBuilder.appendNull();
          currentWriteRowIndex++;
          continue;
        }
        hasAnyNonNullValue[currentWriteRowIndex++] = true;
        switch (dataTypes.get(columnIndex)) {
          case BOOLEAN:
            valueBuilder.writeBoolean(getBooleanByValueIndex(originRowIndex, columnIndex));
            break;
          case INT32:
            valueBuilder.writeInt(getIntByValueIndex(originRowIndex, columnIndex));
            break;
          case DATE:
            if (valueBuilder instanceof BinaryColumnBuilder) {
              ((BinaryColumnBuilder) valueBuilder)
                  .writeDate(getIntByValueIndex(originRowIndex, columnIndex));
            } else {
              valueBuilder.writeInt(getIntByValueIndex(originRowIndex, columnIndex));
            }
            break;
          case INT64:
          case TIMESTAMP:
            valueBuilder.writeLong(getLongByValueIndex(originRowIndex, columnIndex));
            break;
          case FLOAT:
            valueBuilder.writeFloat(
                roundValueWithGivenPrecision(
                    getFloatByValueIndex(originRowIndex, columnIndex),
                    floatPrecision,
                    encodingList.get(columnIndex)));
            break;
          case DOUBLE:
            valueBuilder.writeDouble(
                roundValueWithGivenPrecision(
                    getDoubleByValueIndex(originRowIndex, columnIndex),
                    floatPrecision,
                    encodingList.get(columnIndex)));
            break;
          case TEXT:
          case BLOB:
          case STRING:
          case OBJECT:
            valueBuilder.writeBinary(getBinaryByValueIndex(originRowIndex, columnIndex));
            break;
          default:
            break;
        }
      }
    }
    builder.declarePositions(validRowCount);
    TsBlock tsBlock = builder.build();
    if (!ignoreAllNullRows || !needRebuildTsBlock(hasAnyNonNullValue)) {
      return tsBlock;
    } else {
      // if exist all null rows, at most have validRowCount - 1 valid rows
      return reBuildTsBlock(hasAnyNonNullValue, validRowCount, columnCount, tsBlock);
    }
  }

  // existing any all null row should rebuild the tsblock
  private boolean needRebuildTsBlock(boolean[] hasAnyNonNullValue) {
    for (boolean b : hasAnyNonNullValue) {
      if (!b) {
        return true;
      }
    }
    return false;
  }

  private TsBlock reBuildTsBlock(
      boolean[] hasAnyNonNullValue,
      int previousValidRowCount,
      int columnCount,
      TsBlock previousTsBlock) {
    TsBlockBuilder builder = new TsBlockBuilder(previousValidRowCount - 1, dataTypes);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    Column timeColumn = previousTsBlock.getTimeColumn();
    for (int i = 0; i < previousValidRowCount; i++) {
      if (hasAnyNonNullValue[i]) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        builder.declarePosition();
      }
    }

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      ColumnBuilder columnBuilder = builder.getColumnBuilder(columnIndex);
      Column column = previousTsBlock.getColumn(columnIndex);
      for (int i = 0; i < previousValidRowCount; i++) {
        if (hasAnyNonNullValue[i]) {
          if (column.isNull(i)) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.write(column, i);
          }
        }
      }
    }
    return builder.build();
  }

  protected void writeValidValuesIntoTsBlock(
      TsBlockBuilder builder,
      int floatPrecision,
      TSEncoding encoding,
      List<TimeRange> deletionList) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public int serializedSize() {
    int size = (1 + dataTypes.size()) * Byte.BYTES + 2 * Integer.BYTES;
    // time
    size += rowCount * Long.BYTES;
    // value
    for (int columnIndex = 0; columnIndex < values.size(); ++columnIndex) {
      switch (dataTypes.get(columnIndex)) {
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
            size += ReadWriteIOUtils.sizeToWrite(getBinaryByValueIndex(rowIdx, columnIndex));
          }
          break;
        case FLOAT:
          size += rowCount * Float.BYTES;
          break;
        case INT32:
        case DATE:
          size += rowCount * Integer.BYTES;
          break;
        case INT64:
        case TIMESTAMP:
          size += rowCount * Long.BYTES;
          break;
        case DOUBLE:
          size += rowCount * Double.BYTES;
          break;
        case BOOLEAN:
          size += rowCount * Byte.BYTES;
          break;
        default:
          throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
      }
    }
    // bitmap
    size += rowCount * dataTypes.size() * Byte.BYTES;

    // have timeColDeletedMap
    size += Byte.BYTES;
    if (timeColDeletedMap != null) {
      int length = timeColDeletedMap.getByteArray().length;
      return ReadWriteForEncodingUtils.varIntSize(length) + length * Byte.BYTES;
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(TSDataType.VECTOR, buffer);
    buffer.putInt(dataTypes.size());
    for (TSDataType dataType : dataTypes) {
      buffer.put(dataType.serialize());
    }
    buffer.putInt(rowCount);
    // time
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
      buffer.putLong(getTime(rowIndex));
    }
    // serialize value and bitmap by column
    for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
      List<Object> columnValues = values.get(columnIndex);
      for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        int arrayIndex = rowIndex / ARRAY_SIZE;
        int elementIndex = rowIndex % ARRAY_SIZE;
        // value
        switch (dataTypes.get(columnIndex)) {
          case TEXT:
          case BLOB:
          case STRING:
          case OBJECT:
            Binary valueT = ((Binary[]) columnValues.get(arrayIndex))[elementIndex];
            // In some scenario, the Binary in AlignedTVList will be null if this field is empty in
            // current row. We need to handle this scenario to get rid of NPE. See the similar issue
            // here: https://github.com/apache/iotdb/pull/9884
            // Furthermore, we use an empty Binary as a placeholder here. It won't lead to data
            // error because whether this field is null or not is decided by the bitMap rather than
            // the object's value here.
            if (valueT != null) {
              WALWriteUtils.write(valueT, buffer);
            } else {
              WALWriteUtils.write(new Binary(new byte[0]), buffer);
            }
            break;
          case FLOAT:
            float valueF = ((float[]) columnValues.get(arrayIndex))[elementIndex];
            buffer.putFloat(valueF);
            break;
          case INT32:
          case DATE:
            int valueI = ((int[]) columnValues.get(arrayIndex))[elementIndex];
            buffer.putInt(valueI);
            break;
          case INT64:
          case TIMESTAMP:
            long valueL = ((long[]) columnValues.get(arrayIndex))[elementIndex];
            buffer.putLong(valueL);
            break;
          case DOUBLE:
            double valueD = ((double[]) columnValues.get(arrayIndex))[elementIndex];
            buffer.putDouble(valueD);
            break;
          case BOOLEAN:
            boolean valueB = ((boolean[]) columnValues.get(arrayIndex))[elementIndex];
            WALWriteUtils.write(valueB, buffer);
            break;
          default:
            throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
        }
        // bitmap
        WALWriteUtils.write(isNullValue(rowIndex, columnIndex), buffer);
      }
    }

    if (timeColDeletedMap != null) {
      buffer.put((byte) 1);
      WALWriteUtils.write(timeColDeletedMap.getByteArray().length, buffer);
      buffer.put(timeColDeletedMap.getByteArray());
    } else {
      buffer.put((byte) 0);
    }
  }

  @SuppressWarnings("java:S6541")
  public static AlignedTVList deserialize(DataInputStream stream) throws IOException {
    TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    if (dataType != TSDataType.VECTOR) {
      throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
    int dataTypeNum = stream.readInt();
    List<TSDataType> dataTypes = new ArrayList<>(dataTypeNum);
    for (int columnIndex = 0; columnIndex < dataTypeNum; ++columnIndex) {
      dataTypes.add(ReadWriteIOUtils.readDataType(stream));
    }

    int rowCount = stream.readInt();
    // time
    long[] times = new long[rowCount];
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
      times[rowIndex] = stream.readLong();
    }
    // read value and bitmap by column
    Object[] values = new Object[dataTypeNum];
    BitMap[] bitMaps = new BitMap[dataTypeNum];
    for (int columnIndex = 0; columnIndex < dataTypeNum; ++columnIndex) {
      BitMap bitMap = new BitMap(rowCount);
      Object valuesOfOneColumn;
      switch (dataTypes.get(columnIndex)) {
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          Binary[] binaryValues = new Binary[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            binaryValues[rowIndex] = ReadWriteIOUtils.readBinary(stream);
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = binaryValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            floatValues[rowIndex] = stream.readFloat();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = floatValues;
          break;
        case INT32:
        case DATE:
          int[] intValues = new int[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            intValues[rowIndex] = stream.readInt();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = intValues;
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = new long[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            longValues[rowIndex] = stream.readLong();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = longValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            doubleValues[rowIndex] = stream.readDouble();
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = doubleValues;
          break;
        case BOOLEAN:
          boolean[] booleanValues = new boolean[rowCount];
          for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            booleanValues[rowIndex] = ReadWriteIOUtils.readBool(stream);
            if (ReadWriteIOUtils.readBool(stream)) {
              bitMap.mark(rowIndex);
            }
          }
          valuesOfOneColumn = booleanValues;
          break;
        default:
          throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
      }
      values[columnIndex] = valuesOfOneColumn;
      bitMaps[columnIndex] = bitMap;
    }

    AlignedTVList tvList = AlignedTVList.newAlignedList(new ArrayList<>(dataTypes));
    tvList.putAlignedValues(times, values, bitMaps, 0, rowCount, null);

    boolean hasTimeColDeletedMap = stream.read() == 1;
    if (hasTimeColDeletedMap) {
      int length = ReadWriteForEncodingUtils.readVarInt(stream);
      byte[] bytes = new byte[length];
      stream.readFully(bytes);
      tvList.timeColDeletedMap = new BitMap(rowCount, bytes);
    }

    return tvList;
  }

  public BitMap getTimeColDeletedMap() {
    return timeColDeletedMap;
  }

  /**
   * @param rowIndex should be the sorted index.
   */
  public boolean isTimeDeleted(int rowIndex) {
    return isTimeDeleted(rowIndex, true);
  }

  public boolean isTimeDeleted(int index, boolean needConvertIndex) {
    int bitmapIndex = needConvertIndex ? getValueIndex(index) : index;
    if (timeColDeletedMap == null || timeColDeletedMap.getSize() <= bitmapIndex) {
      return false;
    }
    return timeColDeletedMap.isMarked(bitmapIndex);
  }

  public BitMap getAllValueColDeletedMap() {
    // row exists when any column value exists
    if (bitMaps == null) {
      return null;
    }
    for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
      if (values.get(columnIndex) != null && bitMaps.get(columnIndex) == null) {
        return null;
      }
    }

    byte[] rowBitsArr = new byte[rowCount / Byte.SIZE + 1];
    int bitsMapSize =
        rowCount % ARRAY_SIZE == 0 ? rowCount / ARRAY_SIZE : rowCount / ARRAY_SIZE + 1;
    boolean[] allNotNullArray = new boolean[bitsMapSize];
    Arrays.fill(rowBitsArr, (byte) 0xFF);
    for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
      List<BitMap> columnBitMaps = bitMaps.get(columnIndex);
      if (columnBitMaps == null) {
        Arrays.fill(rowBitsArr, (byte) 0x00);
        break;
      } else if (values.get(columnIndex) != null) {
        int row = 0;
        boolean isEnd = true;
        for (int i = 0; i < bitsMapSize; i++) {
          if (allNotNullArray[i]) {
            row += ARRAY_SIZE;
            continue;
          }

          BitMap bitMap = columnBitMaps.get(i);
          int index = row / Byte.SIZE;
          int size = ((Math.min((rowCount - row), ARRAY_SIZE)) + 7) >>> 3;
          row += ARRAY_SIZE;

          if (bitMap == null) {
            Arrays.fill(rowBitsArr, index, index + size, (byte) 0x00);
            allNotNullArray[i] = true;
            continue;
          }

          byte bits = (byte) 0X00;
          for (int j = 0; j < size; j++) {
            rowBitsArr[index] &= bitMap.getByteArray()[j];
            bits |= rowBitsArr[index++];
            isEnd = false;
          }

          allNotNullArray[i] = bits == (byte) 0;
        }

        if (isEnd) {
          break;
        }
      }
    }
    return new BitMap(rowCount, rowBitsArr);
  }

  public int getAvgPointSizeOfLargestColumn() {
    int largestPrimitivePointSize = 8; // TimeColumn or int64,double ValueColumn
    long largestBinaryChunkSize = 0;
    int largestBinaryColumnIndex = 0;
    for (int i = 0; i < memoryBinaryChunkSize.length; i++) {
      if (memoryBinaryChunkSize[i] > largestBinaryChunkSize) {
        largestBinaryChunkSize = memoryBinaryChunkSize[i];
        largestBinaryColumnIndex = i;
      }
    }
    if (largestBinaryChunkSize == 0) {
      return largestPrimitivePointSize;
    }
    int columnValueCnt = getColumnValueCnt(largestBinaryColumnIndex);
    if (columnValueCnt == 0) {
      return largestPrimitivePointSize;
    }
    int avgPointSizeOfLargestBinaryColumn = (int) largestBinaryChunkSize / columnValueCnt;
    return Math.max(avgPointSizeOfLargestBinaryColumn, largestPrimitivePointSize);
  }

  private int getColumnValueCnt(int columnIndex) {
    int pointNum = 0;
    if (bitMaps == null || bitMaps.get(columnIndex) == null) {
      pointNum = rowCount;
    } else {
      for (int i = 0; i < rowCount; i++) {
        int arrayIndex = i / ARRAY_SIZE;
        if (bitMaps.get(columnIndex).get(arrayIndex) == null) {
          pointNum++;
        } else {
          int elementIndex = i % ARRAY_SIZE;
          if (!bitMaps.get(columnIndex).get(arrayIndex).isMarked(elementIndex)) {
            pointNum++;
          }
        }
      }
    }
    return pointNum;
  }

  public List<List<BitMap>> getBitMaps() {
    return bitMaps;
  }

  public boolean isAllDeleted() {
    return timeDeletedCnt == rowCount;
  }

  public AlignedTVListIterator iterator(
      Ordering scanOrder,
      int rowCount,
      Filter globalTimeFilter,
      List<TSDataType> dataTypeList,
      List<Integer> columnIndexList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    return new AlignedTVListIterator(
        scanOrder,
        rowCount,
        globalTimeFilter,
        dataTypeList,
        columnIndexList,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
  }

  /* AlignedTVList Iterator */
  public class AlignedTVListIterator extends TVListIterator {
    private final BitMap allValueColDeletedMap;
    private final List<TSDataType> dataTypeList;
    private final List<Integer> columnIndexList;
    private final List<TimeRange> timeColumnDeletion;
    private final List<List<TimeRange>> valueColumnsDeletionList;
    private final int floatPrecision;
    private final List<TSEncoding> encodingList;
    private final boolean ignoreAllNullRows;

    // remember the selected index of last not-null value for each column during prepareNext phase
    private final int[] selectedIndices;
    private boolean findValidRow;

    private final int[] timeDeleteCursor = {0};
    private final List<int[]> valueColumnDeleteCursor = new ArrayList<>();

    public AlignedTVListIterator(
        Ordering scanOrder,
        int rowCount,
        Filter globalTimeFilter,
        List<TSDataType> dataTypeList,
        List<Integer> columnIndexList,
        List<TimeRange> timeColumnDeletion,
        List<List<TimeRange>> valueColumnsDeletionList,
        Integer floatPrecision,
        List<TSEncoding> encodingList,
        boolean ignoreAllNullRows,
        int maxNumberOfPointsInPage) {
      super(scanOrder, rowCount, globalTimeFilter, null, null, null, maxNumberOfPointsInPage);
      this.dataTypeList = dataTypeList;
      this.columnIndexList =
          (columnIndexList == null)
              ? IntStream.range(0, dataTypes.size()).boxed().collect(Collectors.toList())
              : columnIndexList;
      this.allValueColDeletedMap = ignoreAllNullRows ? getAllValueColDeletedMap() : null;
      this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
      this.encodingList = encodingList;
      this.timeColumnDeletion = timeColumnDeletion;
      this.valueColumnsDeletionList = valueColumnsDeletionList;
      this.ignoreAllNullRows = ignoreAllNullRows;
      this.selectedIndices = new int[dataTypeList.size()];
      timeDeleteCursor[0] =
          (timeColumnDeletion == null || scanOrder.isAscending())
              ? 0
              : (timeColumnDeletion.size() - 1);
      for (int i = 0; i < dataTypeList.size(); i++) {
        List<TimeRange> valueColumnDeletions =
            valueColumnsDeletionList == null ? null : valueColumnsDeletionList.get(i);
        int cursor =
            (valueColumnDeletions == null || scanOrder.isAscending())
                ? 0
                : (valueColumnDeletions.size() - 1);
        valueColumnDeleteCursor.add(new int[] {cursor});
      }
    }

    @Override
    @SuppressWarnings("java:S6541")
    protected void prepareNext() {
      // find the first row that is neither deleted nor empty (all NULL values)
      findValidRow = false;
      while (index < rows && !findValidRow) {
        // all columns values are deleted
        int convertedScanOrderValueIndex = getValueIndex(getScanOrderIndex(index));
        if ((allValueColDeletedMap != null
                && allValueColDeletedMap.isMarked(convertedScanOrderValueIndex))
            || isTimeDeleted(convertedScanOrderValueIndex, false)) {
          index++;
          continue;
        }
        long time = getTime(getScanOrderIndex(index));
        if (isPointDeleted(time, timeColumnDeletion, timeDeleteCursor, scanOrder)
            || !isTimeSatisfied(time)) {
          index++;
          continue;
        }

        // does not find any valid row
        if (index >= rows) {
          probeNext = true;
          return;
        }
        // When traversing in ASC order, we only need to overwrite the previous non-null value
        // with the non-null value encountered later.
        // When traversing in DESC order, we need to keep the non-null value encountered first,
        // and only overwrite it if the previous value is null and current value is non-null.
        for (int i = 0; i < selectedIndices.length; i++) {
          // In order to identify the previous null, we use index -1 here to represent.
          // The -1 here is also used for checking all null rows
          selectedIndices[i] = isNullValue(index, i) ? -1 : index;
        }
        findValidRow = true;

        // handle duplicated timestamp
        // We can use the selectedIndices structure to handle the value coverage of ASC or DESC
        // traversal
        while (index + 1 < rows
            && getTime(getScanOrderIndex(index + 1)) == getTime(getScanOrderIndex(index))) {
          index++;
          // skip all-Null rows if allValueColDeletedMap exists
          if (allValueColDeletedMap == null
              || !allValueColDeletedMap.isMarked(getValueIndex(getScanOrderIndex(index)))) {
            for (int columnIndex = 0; columnIndex < dataTypeList.size(); columnIndex++) {
              if (!scanOrder.isAscending() && selectedIndices[columnIndex] != -1) {
                // non -1 value means it already set the latest point index
                continue;
              }
              // update selected index if the column is not null
              if (!isNullValue(index, columnIndex)) {
                selectedIndices[columnIndex] = index;
              }
            }
          }
        }

        // valueColumnsDeletionList is set when AlignedTVList iterator is created by
        // MemPointIterator.single method. Otherwise, it is checked by
        // MergeSortMultiAlignedTVListIterator or OrderedMultiAlignedTVListIterator.
        if (ignoreAllNullRows) {
          BitMap bitMap = null;
          time = getTime(getScanOrderIndex(index));
          for (int columnIndex = 0; columnIndex < dataTypeList.size(); columnIndex++) {
            if (selectedIndices[columnIndex] == -1
                || (valueColumnsDeletionList != null
                    && isPointDeleted(
                        time,
                        valueColumnsDeletionList.get(columnIndex),
                        valueColumnDeleteCursor.get(columnIndex),
                        scanOrder))) {
              bitMap = bitMap == null ? new BitMap(dataTypeList.size()) : bitMap;
              bitMap.mark(columnIndex);
            }
          }
          if (bitMap != null && bitMap.isAllMarked()) {
            findValidRow = false;
            index++;
            continue;
          }
        }
        // We previously set some -1. If these values are still -1 in the end,
        // it means that each index is invalid. At this time, we can use any one at random.
        for (int i = 0; i < selectedIndices.length; i++) {
          if (selectedIndices[i] == -1) {
            selectedIndices[i] = index;
          }
        }
      }
      probeNext = true;
    }

    // When used as a point reader, we should not apply a pagination controller or push down filter
    // because it has not yet been merged with other data.
    @Override
    public TimeValuePair nextTimeValuePair() {
      if (!hasNextTimeValuePair()) {
        return null;
      }
      TsPrimitiveType[] vector = new TsPrimitiveType[dataTypeList.size()];
      for (int columnIndex = 0; columnIndex < dataTypeList.size(); columnIndex++) {
        vector[columnIndex] = getPrimitiveTypeObject(selectedIndices[columnIndex], columnIndex);
      }
      TimeValuePair tvPair =
          new TimeValuePair(
              getTime(getScanOrderIndex(index)),
              TsPrimitiveType.getByType(TSDataType.VECTOR, vector));

      next();
      return tvPair;
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      if (!hasCurrent()) {
        return null;
      }
      TsPrimitiveType[] vector = new TsPrimitiveType[dataTypeList.size()];
      for (int columnIndex = 0; columnIndex < dataTypeList.size(); columnIndex++) {
        vector[columnIndex] = getPrimitiveTypeObject(selectedIndices[columnIndex], columnIndex);
      }
      return new TimeValuePair(
          getTime(getScanOrderIndex(index)), TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
    }

    public TsPrimitiveType getPrimitiveTypeObject(int rowIndex, int columnIndex) {
      rowIndex = getScanOrderIndex(rowIndex);
      int valueIndex = getValueIndex(rowIndex);
      if (valueIndex < 0 || valueIndex >= rows) {
        return null;
      }
      int validColumnIndex = columnIndexList.get(columnIndex);
      if (validColumnIndex < 0 || validColumnIndex >= dataTypes.size()) {
        return null;
      }
      if (outer.isNullValue(valueIndex, validColumnIndex)) {
        return null;
      }
      switch (dataTypeList.get(columnIndex)) {
        case BOOLEAN:
          return TsPrimitiveType.getByType(
              TSDataType.BOOLEAN, getBooleanByValueIndex(valueIndex, validColumnIndex));
        case INT32:
          return TsPrimitiveType.getByType(
              TSDataType.INT32, getIntByValueIndex(valueIndex, validColumnIndex));
        case DATE:
          return TsPrimitiveType.getByType(
              TSDataType.DATE, getIntByValueIndex(valueIndex, validColumnIndex));
        case INT64:
        case TIMESTAMP:
          return TsPrimitiveType.getByType(
              TSDataType.INT64, getLongByValueIndex(valueIndex, validColumnIndex));
        case FLOAT:
          float valueF = getFloatByValueIndex(valueIndex, validColumnIndex);
          if (encodingList != null) {
            valueF =
                roundValueWithGivenPrecision(valueF, floatPrecision, encodingList.get(columnIndex));
          }
          return TsPrimitiveType.getByType(TSDataType.FLOAT, valueF);
        case DOUBLE:
          double valueD = getDoubleByValueIndex(valueIndex, validColumnIndex);
          if (encodingList != null) {
            valueD =
                roundValueWithGivenPrecision(valueD, floatPrecision, encodingList.get(columnIndex));
          }
          return TsPrimitiveType.getByType(TSDataType.DOUBLE, valueD);
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          return TsPrimitiveType.getByType(
              TSDataType.TEXT, getBinaryByValueIndex(valueIndex, validColumnIndex));
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataTypeList.get(columnIndex)));
      }
    }

    @Override
    @SuppressWarnings("java:S6541")
    public boolean hasNextBatch() {
      if (!paginationController.hasCurLimit()) {
        return false;
      }
      if (!probeNext) {
        prepareNext();
      }
      if (findValidRow && selectedIndices.length > 0) {
        index = Arrays.stream(selectedIndices).min().getAsInt();
      }
      return index < rows && !isCurrentTimeExceedTimeRange(getTime(getScanOrderIndex(index)));
    }

    @Override
    public TsBlock nextBatch() {
      int maxRowCountOfCurrentBatch = Math.min(rows - index, maxNumberOfPointsInPage);
      TsBlockBuilder builder = new TsBlockBuilder(maxRowCountOfCurrentBatch, dataTypeList);
      // Time column
      TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();

      int validRowCount = 0;
      // Rows that are deleted or whose timestamps do not match the filter are considered invalid.
      // The corresponding bit is set to true if the row is not needed.
      LazyBitMap timeInvalidInfo = null;
      LazyBitMap timeDuplicatedInfo = null;

      int[] deleteCursor = {0};
      int startIndex = index;
      // time column
      for (; index < rows; index++) {
        long time = getTime(getScanOrderIndex(index));
        if (validRowCount >= maxNumberOfPointsInPage || isCurrentTimeExceedTimeRange(time)) {
          break;
        }
        // skip invalid row
        if ((allValueColDeletedMap != null
                && allValueColDeletedMap.isMarked(getValueIndex(getScanOrderIndex(index))))
            || isTimeDeleted(getScanOrderIndex(index))
            || !isTimeSatisfied(time)
            || isPointDeleted(time, timeColumnDeletion, deleteCursor, scanOrder)) {
          timeInvalidInfo =
              timeInvalidInfo == null
                  ? new LazyBitMap(index, maxRowCountOfCurrentBatch, rows - 1)
                  : timeInvalidInfo;
          timeInvalidInfo.mark(index);
          continue;
        }
        int nextRowIndex = index + 1;
        long timeOfNextRowIndex;
        while (nextRowIndex < rows
            && ((allValueColDeletedMap != null
                    && allValueColDeletedMap.isMarked(
                        getValueIndex(getScanOrderIndex(nextRowIndex))))
                || isTimeDeleted(getScanOrderIndex(nextRowIndex))
                || !isTimeSatisfied((timeOfNextRowIndex = getTime(getScanOrderIndex(nextRowIndex))))
                || isPointDeleted(
                    timeOfNextRowIndex, timeColumnDeletion, deleteCursor, scanOrder))) {
          timeInvalidInfo =
              timeInvalidInfo == null
                  ? new LazyBitMap(nextRowIndex, maxRowCountOfCurrentBatch, rows - 1)
                  : timeInvalidInfo;
          timeInvalidInfo.mark(nextRowIndex);
          nextRowIndex++;
        }
        if ((nextRowIndex == rows || time != getTime(getScanOrderIndex(nextRowIndex)))) {
          timeBuilder.writeLong(time);
          validRowCount++;
        } else {
          if (Objects.isNull(timeDuplicatedInfo)) {
            timeDuplicatedInfo = new LazyBitMap(index, maxRowCountOfCurrentBatch, rows - 1);
          }
          // For this timeDuplicatedInfo, we mark all positions that are not the last one in the
          // ASC traversal. It has the same behaviour for the DESC traversal, because our ultimate
          // goal is to process all the data with the same timestamp before writing it into TsBlock.
          timeDuplicatedInfo.mark(index);
        }
        index = nextRowIndex - 1;
      }

      boolean[] hasAnyNonNullValue = new boolean[validRowCount];
      int columnCount = dataTypeList.size();
      int currentWriteRowIndex;
      // value columns
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        int validColumnIndex = columnIndexList.get(columnIndex);

        deleteCursor = new int[] {0};
        // Pair of Time and Index
        Pair<Long, Integer> lastValidPointIndexForTimeDupCheck = null;
        if (Objects.nonNull(timeDuplicatedInfo)) {
          lastValidPointIndexForTimeDupCheck = new Pair<>(Long.MIN_VALUE, null);
        }
        ColumnBuilder valueBuilder = builder.getColumnBuilder(columnIndex);
        currentWriteRowIndex = 0;
        for (int sortedRowIndex = startIndex; sortedRowIndex < index; sortedRowIndex++) {
          // skip invalid rows
          if (Objects.nonNull(timeInvalidInfo)) {
            if (timeInvalidInfo.isMarked(sortedRowIndex)) {
              continue;
            }
          }
          // skip time duplicated rows
          if (Objects.nonNull(timeDuplicatedInfo)) {
            if (!outer.isNullValue(
                getValueIndex(getScanOrderIndex(sortedRowIndex)), validColumnIndex)) {
              lastValidPointIndexForTimeDupCheck.left = getTime(getScanOrderIndex(sortedRowIndex));
              if (scanOrder.isAscending()) {
                lastValidPointIndexForTimeDupCheck.right =
                    getValueIndex(getScanOrderIndex(sortedRowIndex));
              } else if (lastValidPointIndexForTimeDupCheck.right == null) {
                // For DESC traversal, we need to keep the first non-null value encountered
                // We can use lastValidPointIndexForTimeDupCheck.right as a judgment method to see
                // if it is null
                lastValidPointIndexForTimeDupCheck.right =
                    getValueIndex(getScanOrderIndex(sortedRowIndex));
              }
            }
            // timeDuplicatedInfo was constructed when traversing the time column before. It can be
            // reused when traversing each value column to skip non-last rows with
            // duplicated timestamps.
            // Until the last duplicate timestamp is encountered, it will be skipped here.
            if (timeDuplicatedInfo.isMarked(sortedRowIndex)) {
              continue;
            }
          }

          // append null value when query column does not exist in current aligned TVList
          if (validColumnIndex < 0 || validColumnIndex >= dataTypes.size()) {
            valueBuilder.appendNull();
            currentWriteRowIndex++;
            continue;
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
          if (Objects.nonNull(lastValidPointIndexForTimeDupCheck)
              && (getTime(getScanOrderIndex(sortedRowIndex))
                  == lastValidPointIndexForTimeDupCheck.left)
              && Objects.nonNull(lastValidPointIndexForTimeDupCheck.right)) {
            originRowIndex = lastValidPointIndexForTimeDupCheck.right;
            // For DESC traversal, the judgment of whether the previous point is null depends on
            // lastValidPointIndexForTimeDupCheck.right, so we need to remember to clean it up
            // after writing a point
            lastValidPointIndexForTimeDupCheck.right = null;
          } else {
            originRowIndex = getValueIndex(getScanOrderIndex(sortedRowIndex));
          }
          if (outer.isNullValue(originRowIndex, validColumnIndex)
              || isPointDeleted(
                  getTime(getScanOrderIndex(sortedRowIndex)),
                  Objects.isNull(valueColumnsDeletionList)
                      ? null
                      : valueColumnsDeletionList.get(columnIndex),
                  deleteCursor,
                  scanOrder)) {
            valueBuilder.appendNull();
            currentWriteRowIndex++;
            continue;
          }
          hasAnyNonNullValue[currentWriteRowIndex++] = true;
          writeToColumn(validColumnIndex, valueBuilder, originRowIndex, columnIndex);
        }
      }
      builder.declarePositions(validRowCount);
      TsBlock tsBlock = builder.build();
      if (ignoreAllNullRows && needRebuildTsBlock(hasAnyNonNullValue)) {
        // if exist all null rows, at most have validRowCount - 1 valid rows
        // When rebuilding TsBlock, pushDownFilter and paginationController are also processed.
        tsBlock = reBuildTsBlock(hasAnyNonNullValue, validRowCount, dataTypeList, tsBlock);
      } else if (pushDownFilter != null) {
        tsBlock =
            TsBlockUtil.applyFilterAndLimitOffsetToTsBlock(
                tsBlock,
                new TsBlockBuilder(
                    Math.min(maxNumberOfPointsInPage, tsBlock.getPositionCount()), dataTypeList),
                pushDownFilter,
                paginationController);
      } else {
        tsBlock = paginationController.applyTsBlock(tsBlock);
      }
      addTsBlock(tsBlock);

      probeNext = false;
      return tsBlock;
    }

    private void writeToColumn(
        int validColumnIndex, ColumnBuilder valueBuilder, int originRowIndex, int columnIndex) {
      switch (dataTypes.get(validColumnIndex)) {
        case BOOLEAN:
          valueBuilder.writeBoolean(getBooleanByValueIndex(originRowIndex, validColumnIndex));
          break;
        case INT32:
          valueBuilder.writeInt(getIntByValueIndex(originRowIndex, validColumnIndex));
          break;
        case DATE:
          if (valueBuilder instanceof BinaryColumnBuilder) {
            ((BinaryColumnBuilder) valueBuilder)
                .writeDate(getIntByValueIndex(originRowIndex, validColumnIndex));
          } else {
            valueBuilder.writeInt(getIntByValueIndex(originRowIndex, validColumnIndex));
          }
          break;
        case INT64:
        case TIMESTAMP:
          valueBuilder.writeLong(getLongByValueIndex(originRowIndex, validColumnIndex));
          break;
        case FLOAT:
          float valueF = getFloatByValueIndex(originRowIndex, validColumnIndex);
          if (encodingList != null) {
            valueF =
                roundValueWithGivenPrecision(valueF, floatPrecision, encodingList.get(columnIndex));
          }
          valueBuilder.writeFloat(valueF);
          break;
        case DOUBLE:
          double valueD = getDoubleByValueIndex(originRowIndex, validColumnIndex);
          if (encodingList != null) {
            valueD =
                roundValueWithGivenPrecision(valueD, floatPrecision, encodingList.get(columnIndex));
          }
          valueBuilder.writeDouble(valueD);
          break;
        case TEXT:
        case BLOB:
        case STRING:
        case OBJECT:
          valueBuilder.writeBinary(getBinaryByValueIndex(originRowIndex, validColumnIndex));
          break;
        default:
          break;
      }
    }

    private TsBlock reBuildTsBlock(
        boolean[] hasAnyNonNullValue,
        int previousValidRowCount,
        List<TSDataType> tsDataTypeList,
        TsBlock previousTsBlock) {
      boolean[] selection = hasAnyNonNullValue;
      if (pushDownFilter != null) {
        selection = pushDownFilter.satisfyTsBlock(hasAnyNonNullValue, previousTsBlock);
      }
      TsBlockBuilder builder = new TsBlockBuilder(previousValidRowCount - 1, tsDataTypeList);
      TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
      Column timeColumn = previousTsBlock.getTimeColumn();
      int stopIndex = previousValidRowCount;
      for (int i = 0; i < previousValidRowCount; i++) {
        if (selection[i]) {
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            selection[i] = false;
          } else if (paginationController.hasCurLimit()) {
            timeColumnBuilder.writeLong(timeColumn.getLong(i));
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            stopIndex = i;
            break;
          }
        }
      }

      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        ColumnBuilder columnBuilder = builder.getColumnBuilder(columnIndex);
        Column column = previousTsBlock.getColumn(columnIndex);
        for (int i = 0; i < stopIndex; i++) {
          if (selection[i]) {
            if (column.isNull(i)) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.write(column, i);
            }
          }
        }
      }
      return builder.build();
    }

    @Override
    @SuppressWarnings("java:S6541")
    public void encodeBatch(IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times) {
      int maxRowCountOfCurrentBatch =
          Math.min(
              rows - index,
              Math.min(
                  (int) encodeInfo.maxNumberOfPointsInChunk - encodeInfo.pointNumInChunk, // NOSONAR
                  encodeInfo.maxNumberOfPointsInPage - encodeInfo.pointNumInPage));
      AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;

      // duplicated time or deleted time are all invalid, true if we don't need this row
      LazyBitMap timeDuplicateInfo = null;

      int startIndex = index;
      // time column
      for (; index < rows; index++) {
        if (encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk
            || encodeInfo.pointNumInPage >= encodeInfo.maxNumberOfPointsInPage) {
          break;
        }
        // skip empty row
        if (allValueColDeletedMap != null && allValueColDeletedMap.isMarked(getValueIndex(index))) {
          continue;
        }
        if (isTimeDeleted(index)) {
          continue;
        }
        int nextRowIndex = index + 1;
        while (nextRowIndex < rows
            && ((allValueColDeletedMap != null
                    && allValueColDeletedMap.isMarked(getValueIndex(nextRowIndex)))
                || (isTimeDeleted(nextRowIndex)))) {
          nextRowIndex++;
        }
        long time = getTime(index);
        if (nextRowIndex == rows || time != getTime(nextRowIndex)) {
          times[encodeInfo.pointNumInPage++] = time;
          encodeInfo.pointNumInChunk++;
        } else {
          if (Objects.isNull(timeDuplicateInfo)) {
            timeDuplicateInfo = new LazyBitMap(index, maxRowCountOfCurrentBatch, rows - 1);
          }
          timeDuplicateInfo.mark(index);
        }
        index = nextRowIndex - 1;
      }

      int columnCount = dataTypeList.size();
      // value columns
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        ValueChunkWriter valueChunkWriter =
            alignedChunkWriter.getValueChunkWriterByIndex(columnIndex);
        int validColumnIndex = columnIndexList.get(columnIndex);

        // Pair of Time and Index
        Pair<Long, Integer> lastValidPointIndexForTimeDupCheck = null;
        if (Objects.nonNull(timeDuplicateInfo)) {
          lastValidPointIndexForTimeDupCheck = new Pair<>(Long.MIN_VALUE, null);
        }
        for (int sortedRowIndex = startIndex; sortedRowIndex < index; sortedRowIndex++) {
          // skip empty row
          if ((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(getValueIndex(sortedRowIndex)))
              || (isTimeDeleted(sortedRowIndex))) {
            continue;
          }
          long time = getTime(sortedRowIndex);
          // skip time duplicated or totally deleted rows
          if (Objects.nonNull(timeDuplicateInfo)) {
            if (!outer.isNullValue(getValueIndex(sortedRowIndex), validColumnIndex)) {
              lastValidPointIndexForTimeDupCheck.left = getTime(sortedRowIndex);
              lastValidPointIndexForTimeDupCheck.right = getValueIndex(sortedRowIndex);
            }
            if (timeDuplicateInfo.isMarked(sortedRowIndex)) {
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
          if (Objects.nonNull(lastValidPointIndexForTimeDupCheck)
              && (getTime(sortedRowIndex) == lastValidPointIndexForTimeDupCheck.left)) {
            originRowIndex = lastValidPointIndexForTimeDupCheck.right;
          } else {
            originRowIndex = getValueIndex(sortedRowIndex);
          }

          boolean isNull = outer.isNullValue(originRowIndex, validColumnIndex);
          switch (dataTypeList.get(columnIndex)) {
            case BOOLEAN:
              valueChunkWriter.write(
                  time,
                  !isNull && getBooleanByValueIndex(originRowIndex, validColumnIndex),
                  isNull);
              break;
            case INT32:
            case DATE:
              valueChunkWriter.write(
                  time, isNull ? 0 : getIntByValueIndex(originRowIndex, validColumnIndex), isNull);
              break;
            case INT64:
            case TIMESTAMP:
              valueChunkWriter.write(
                  time, isNull ? 0 : getLongByValueIndex(originRowIndex, validColumnIndex), isNull);
              break;
            case FLOAT:
              valueChunkWriter.write(
                  time,
                  isNull ? 0 : getFloatByValueIndex(originRowIndex, validColumnIndex),
                  isNull);
              break;
            case DOUBLE:
              valueChunkWriter.write(
                  time,
                  isNull ? 0 : getDoubleByValueIndex(originRowIndex, validColumnIndex),
                  isNull);
              break;
            case TEXT:
            case BLOB:
            case STRING:
            case OBJECT:
              valueChunkWriter.write(
                  time,
                  isNull ? null : getBinaryByValueIndex(originRowIndex, validColumnIndex),
                  isNull);
              break;
            default:
              break;
          }
        }
      }
      probeNext = false;
    }

    public int[] getSelectedIndices() {
      return selectedIndices;
    }

    public int getSelectedIndex(int column) {
      return selectedIndices[column];
    }

    public AlignedTVList getAlignedTVList() {
      return outer;
    }

    /**
     * @param rowIndex index of sorted rows in the Aligned TVList.
     * @param columnIndex index of columnIndexList for the query.
     * @return boolean
     */
    public boolean isNullValue(int rowIndex, int columnIndex) {
      // valueIndex is converted index of values
      int valueIndex = getValueIndex(getScanOrderIndex(rowIndex));
      // validColumnIndex is converted index of columns in the Aligned TVList.
      int validColumnIndex = columnIndexList.get(columnIndex);
      if (validColumnIndex < 0 || validColumnIndex >= dataTypes.size()) {
        return true;
      }
      return outer.isNullValue(valueIndex, validColumnIndex);
    }
  }
}
