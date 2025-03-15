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
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

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
      values.add(new ArrayList<>());
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

  @Override
  public TVList getTvListByColumnIndex(
      List<Integer> columnIndex, List<TSDataType> dataTypeList, boolean ignoreAllNullRows) {
    List<List<Object>> values = new ArrayList<>();
    List<List<BitMap>> bitMaps = null;
    for (int i = 0; i < columnIndex.size(); i++) {
      // columnIndex == -1 means querying a non-exist column, add null column here
      if (columnIndex.get(i) == -1) {
        values.add(null);
      } else {
        values.add(this.values.get(columnIndex.get(i)));
        if (this.bitMaps != null && this.bitMaps.get(columnIndex.get(i)) != null) {
          if (bitMaps == null) {
            bitMaps = new ArrayList<>(columnIndex.size());
            for (int j = 0; j < columnIndex.size(); j++) {
              bitMaps.add(null);
            }
          }
          bitMaps.set(i, this.bitMaps.get(columnIndex.get(i)));
        }
      }
    }
    AlignedTVList alignedTvList = AlignedTVList.newAlignedList(dataTypeList);
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
  public AlignedTVList clone() {
    AlignedTVList cloneList = AlignedTVList.newAlignedList(dataTypes);
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
          List<BitMap> cloneColumnBitMaps = new ArrayList<>();
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
      bitMaps = new ArrayList<>(values.size());
      for (int i = 0; i < values.size(); i++) {
        bitMaps.add(null);
      }
    }
    List<Object> columnValue = new ArrayList<>();
    List<BitMap> columnBitMaps = new ArrayList<>();
    for (int i = 0; i < timestamps.size(); i++) {
      switch (dataType) {
        case TEXT:
        case STRING:
        case BLOB:
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
        for (int j = 0; j < rowCount % ARRAY_SIZE; j++) {
          bitMap.mark(j);
        }
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
    return ((int[]) columnValues.get(arrayIndex))[elementIndex];
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
    return ((long[]) columnValues.get(arrayIndex))[elementIndex];
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
    return ((float[]) columnValues.get(arrayIndex))[elementIndex];
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
    return ((double[]) columnValues.get(arrayIndex))[elementIndex];
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
    return ((Binary[]) columnValues.get(arrayIndex))[elementIndex];
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
    return ((boolean[]) columnValues.get(arrayIndex))[elementIndex];
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
        markNullValue(columnIndex, arrayIndex, elementIndex);
        deletedNumber++;
      } else {
        deleteColumn = false;
      }
    }
    return new Pair<>(deletedNumber, deleteColumn);
  }

  public void deleteColumn(int columnIndex) {
    if (bitMaps == null) {
      bitMaps = new ArrayList<>(dataTypes.size());
      for (int j = 0; j < dataTypes.size(); j++) {
        bitMaps.add(null);
      }
    }
    if (bitMaps.get(columnIndex) == null) {
      List<BitMap> columnBitMaps = new ArrayList<>();
      for (int i = 0; i < values.get(columnIndex).size(); i++) {
        columnBitMaps.add(new BitMap(ARRAY_SIZE));
      }
      bitMaps.set(columnIndex, columnBitMaps);
    }
    for (int i = 0; i < bitMaps.get(columnIndex).size(); i++) {
      bitMaps.get(columnIndex).get(i).markAll();
    }
  }

  protected Object cloneValue(TSDataType type, Object value) {
    switch (type) {
      case TEXT:
      case BLOB:
      case STRING:
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
          for (int j = 0; j < values.size(); j++) {
            if (value[j] == null
                || bitMaps != null && bitMaps[j] != null && bitMaps[j].isMarked(idx + i)
                || results != null
                    && results[idx + i] != null
                    && results[idx + i].code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              markNullValue(j, arrayIdx, elementIdx + i);
            }
          }
          rowCount++;
        }
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
          for (int j = 0; j < values.size(); j++) {
            if (value[j] == null
                || bitMaps != null && bitMaps[j] != null && bitMaps[j].isMarked(idx + i)
                || results != null
                    && results[idx + i] != null
                    && results[idx + i].code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              markNullValue(j, arrayIdx, elementIdx + i);
            }
          }
          rowCount++;
        }
        idx += internalRemaining;
        checkExpansion();
      }
    }
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

  private void markNullValue(int columnIndex, int arrayIndex, int elementIndex) {
    // init BitMaps if doesn't have
    if (bitMaps == null) {
      bitMaps = new ArrayList<>(dataTypes.size());
      for (int i = 0; i < dataTypes.size(); i++) {
        bitMaps.add(null);
      }
    }

    // if the bitmap in columnIndex is null, init the bitmap of this column from the beginning
    if (bitMaps.get(columnIndex) == null) {
      List<BitMap> columnBitMaps = new ArrayList<>();
      for (int i = 0; i < values.get(columnIndex).size(); i++) {
        columnBitMaps.add(new BitMap(ARRAY_SIZE));
      }
      bitMaps.set(columnIndex, columnBitMaps);
    }

    // if the bitmap in arrayIndex is null, init the bitmap
    if (bitMaps.get(columnIndex).get(arrayIndex) == null) {
      bitMaps.get(columnIndex).set(arrayIndex, new BitMap(ARRAY_SIZE));
    }

    // mark the null value in the current bitmap
    bitMaps.get(columnIndex).get(arrayIndex).mark(elementIndex);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.VECTOR;
  }

  @Override
  public long calculateRamSize() {
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
          case DATE:
            valueBuilder.writeInt(getIntByValueIndex(originRowIndex, columnIndex));
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

    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
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
    for (int row = 0; row < rowCount; row += Byte.SIZE) {
      boolean isFirstColumn = true;
      byte rowBits = 0x00;
      for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
        List<BitMap> columnBitMaps = bitMaps.get(columnIndex);
        byte columnBits;
        if (values.get(columnIndex) == null) {
          columnBits = (byte) 0xFF;
        } else if (columnBitMaps == null || columnBitMaps.get(row / ARRAY_SIZE) == null) {
          // row exists when any column value exists
          rowBits = 0x00;
          break;
        } else {
          columnBits =
              columnBitMaps.get(row / ARRAY_SIZE).getByteArray()[(row % ARRAY_SIZE) / Byte.SIZE];
        }
        // set row to null when all column values are null
        if (isFirstColumn) {
          rowBits = columnBits;
          isFirstColumn = false;
        } else {
          rowBits &= columnBits;
        }
      }
      rowBitsArr[row / Byte.SIZE] = rowBits;
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
      List<TSDataType> dataTypeList,
      List<Integer> columnIndexList,
      List<TimeRange> timeColumnDeletion,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    return new AlignedTVListIterator(
        dataTypeList,
        columnIndexList,
        timeColumnDeletion,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
  }

  /* AlignedTVList Iterator */
  public class AlignedTVListIterator extends TVListIterator {
    private final BitMap allValueColDeletedMap;
    private final List<TSDataType> dataTypeList;
    private final List<Integer> columnIndexList;
    private final List<TimeRange> timeColumnDeletion;
    private final Integer floatPrecision;
    private final List<TSEncoding> encodingList;

    // remember the selected index of last not-null value for each column during prepareNext phase
    private final int[] selectedIndices;

    int[] timeDeleteCursor = {0};

    public AlignedTVListIterator(
        List<TSDataType> dataTypeList,
        List<Integer> columnIndexList,
        List<TimeRange> timeColumnDeletion,
        Integer floatPrecision,
        List<TSEncoding> encodingList,
        boolean ignoreAllNullRows) {
      super(null);
      this.dataTypeList = dataTypeList;
      this.columnIndexList =
          (columnIndexList == null)
              ? IntStream.range(0, dataTypes.size()).boxed().collect(Collectors.toList())
              : columnIndexList;
      this.allValueColDeletedMap = ignoreAllNullRows ? getAllValueColDeletedMap() : null;
      this.floatPrecision = floatPrecision;
      this.encodingList = encodingList;
      this.timeColumnDeletion = timeColumnDeletion;
      this.selectedIndices = new int[dataTypeList.size()];
    }

    @Override
    protected void prepareNext() {
      // find the first row that is neither deleted nor empty (all NULL values)
      boolean findValidRow = false;
      while (index < rows && !findValidRow) {
        int rowIndex = getValueIndex(index);
        // all columns values are deleted
        if ((allValueColDeletedMap != null && allValueColDeletedMap.isMarked(rowIndex))
            || isTimeDeleted(rowIndex, false)
            || isPointDeleted(currentTime, timeColumnDeletion, timeDeleteCursor)) {
          index++;
          currentTime = index < rows ? getTime(index) : Long.MIN_VALUE;
          continue;
        }

        // does not find any valid row
        if (index >= rows) {
          probeNext = true;
          return;
        }
        Arrays.fill(selectedIndices, rowIndex);
        findValidRow = true;
      }

      // handle duplicated timestamp
      while (index + 1 < rows && getTime(index + 1) == currentTime) {
        index++;
        // skip all-Null rows if allValueColDeletedMap exists
        int rowIndex = getValueIndex(index);
        if (allValueColDeletedMap == null || !allValueColDeletedMap.isMarked(rowIndex)) {
          for (int columnIndex = 0; columnIndex < dataTypeList.size(); columnIndex++) {
            // update selected index if the column is not null
            if (!isNullValue(rowIndex, columnIndex)) {
              selectedIndices[columnIndex] = rowIndex;
            }
          }
        }
      }
      probeNext = true;
    }

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
          new TimeValuePair(currentTime, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));

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
      return new TimeValuePair(currentTime, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
    }

    public TsPrimitiveType getPrimitiveTypeObject(int rowIndex, int columnIndex) {
      if (rowIndex < 0 || rowIndex >= rows) {
        return null;
      }
      int validColumnIndex = columnIndexList.get(columnIndex);
      if (validColumnIndex < 0 || validColumnIndex >= dataTypes.size()) {
        return null;
      }
      if (outer.isNullValue(rowIndex, validColumnIndex)) {
        return null;
      }
      switch (dataTypeList.get(columnIndex)) {
        case BOOLEAN:
          return TsPrimitiveType.getByType(
              TSDataType.BOOLEAN, getBooleanByValueIndex(rowIndex, validColumnIndex));
        case INT32:
        case DATE:
          return TsPrimitiveType.getByType(
              TSDataType.INT32, getIntByValueIndex(rowIndex, validColumnIndex));
        case INT64:
        case TIMESTAMP:
          return TsPrimitiveType.getByType(
              TSDataType.INT64, getLongByValueIndex(rowIndex, validColumnIndex));
        case FLOAT:
          return TsPrimitiveType.getByType(
              TSDataType.FLOAT,
              roundValueWithGivenPrecision(
                  getFloatByValueIndex(rowIndex, validColumnIndex),
                  floatPrecision,
                  encodingList.get(columnIndex)));
        case DOUBLE:
          return TsPrimitiveType.getByType(
              TSDataType.DOUBLE,
              roundValueWithGivenPrecision(
                  getDoubleByValueIndex(rowIndex, validColumnIndex),
                  floatPrecision,
                  encodingList.get(columnIndex)));
        case TEXT:
        case BLOB:
        case STRING:
          return TsPrimitiveType.getByType(
              TSDataType.TEXT, getBinaryByValueIndex(rowIndex, validColumnIndex));
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataTypeList.get(columnIndex)));
      }
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

    public boolean isNullValue(int rowIndex, int columnIndex) {
      int validColumnIndex = columnIndexList.get(columnIndex);
      if (validColumnIndex < 0 || validColumnIndex >= dataTypes.size()) {
        return true;
      }
      return outer.isNullValue(rowIndex, validColumnIndex);
    }
  }
}
