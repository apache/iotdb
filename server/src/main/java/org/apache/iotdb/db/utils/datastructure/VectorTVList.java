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

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class VectorTVList extends TVList {

  // data types of this vector
  private List<TSDataType> dataTypes;

  // data type list -> list of TVList, add 1 when expanded -> primitive array of basic type
  // index relation: columnIndex(dataTypeIndex) -> arrayIndex -> elementIndex
  private List<List<Object>> values;

  // list of index array, add 1 when expanded -> data point index array
  // index relation: arrayIndex -> elementIndex
  // used in sort method, sort only changes indices
  private List<int[]> indices;

  // data type list -> list of BitMap, add 1 when expanded -> BitMap(maybe null), marked means the
  // value is null
  // index relation: columnIndex(dataTypeIndex) -> arrayIndex -> elementIndex
  private List<List<BitMap>> bitMaps;

  private int[][] sortedIndices;

  private int pivotIndex;

  VectorTVList(List<TSDataType> types) {
    super();
    indices = new ArrayList<>(types.size());
    dataTypes = types;
    values = new ArrayList<>(types.size());
    for (int i = 0; i < types.size(); i++) {
      values.add(new ArrayList<>());
    }
  }

  public static VectorTVList newVectorList(List<TSDataType> datatypes) {
    return new VectorTVList(datatypes);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public void putVector(long timestamp, Object[] value, int[] columnOrder) {
    checkExpansion();
    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    minTime = Math.min(minTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    for (int i = 0; i < values.size(); i++) {
      Object columnValue = value[columnOrder[i]];
      List<Object> columnValues = values.get(i);
      if (columnValue == null) {
        markNullValue(i, arrayIndex, elementIndex);
      }
      switch (dataTypes.get(i)) {
        case TEXT:
          ((Binary[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (Binary) columnValue : Binary.EMPTY_VALUE;
          break;
        case FLOAT:
          ((float[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (float) columnValue : Float.MIN_VALUE;
          break;
        case INT32:
          ((int[]) columnValues.get(arrayIndex))[elementIndex] =
              columnValue != null ? (int) columnValue : Integer.MIN_VALUE;
          break;
        case INT64:
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
    indices.get(arrayIndex)[elementIndex] = size;
    size++;
    if (sorted && size > 1 && timestamp < getTime(size - 2)) {
      sorted = false;
    }
  }

  @Override
  public Object getVector(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getVectorByValueIndex(valueIndex, null);
  }

  public TsPrimitiveType getVector(List<Integer> timeDuplicatedIndexList) {
    int[] validIndexesForTimeDuplicatedRows = new int[values.size()];
    for (int i = 0; i < values.size(); i++) {
      validIndexesForTimeDuplicatedRows[i] =
          getValidRowIndexForTimeDuplicatedRows(timeDuplicatedIndexList, i);
    }
    return getVectorByValueIndex(
        timeDuplicatedIndexList.get(timeDuplicatedIndexList.size() - 1),
        validIndexesForTimeDuplicatedRows);
  }

  private TsPrimitiveType getVectorByValueIndex(
      int valueIndex, int[] validIndexesForTimeDuplicatedRows) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    TsPrimitiveType[] vector = new TsPrimitiveType[values.size()];
    for (int columnIndex = 0; columnIndex < values.size(); columnIndex++) {
      List<Object> columnValues = values.get(columnIndex);
      if (validIndexesForTimeDuplicatedRows != null) {
        arrayIndex = validIndexesForTimeDuplicatedRows[columnIndex] / ARRAY_SIZE;
        elementIndex = validIndexesForTimeDuplicatedRows[columnIndex] % ARRAY_SIZE;
      }
      if (bitMaps != null
          && bitMaps.get(columnIndex) != null
          && isValueMarked(valueIndex, columnIndex)) {
        continue;
      }
      switch (dataTypes.get(columnIndex)) {
        case TEXT:
          vector[columnIndex] =
              TsPrimitiveType.getByType(
                  dataTypes.get(columnIndex),
                  ((Binary[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case FLOAT:
          vector[columnIndex] =
              TsPrimitiveType.getByType(
                  dataTypes.get(columnIndex),
                  ((float[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case INT32:
          vector[columnIndex] =
              TsPrimitiveType.getByType(
                  dataTypes.get(columnIndex), ((int[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case INT64:
          vector[columnIndex] =
              TsPrimitiveType.getByType(
                  dataTypes.get(columnIndex),
                  ((long[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case DOUBLE:
          vector[columnIndex] =
              TsPrimitiveType.getByType(
                  dataTypes.get(columnIndex),
                  ((double[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case BOOLEAN:
          vector[columnIndex] =
              TsPrimitiveType.getByType(
                  dataTypes.get(columnIndex),
                  ((boolean[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        default:
          throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
      }
    }
    return TsPrimitiveType.getByType(TSDataType.VECTOR, vector);
  }

  @Override
  public TVList getTvListByColumnIndex(List<Integer> columns) {
    List<TSDataType> types = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    List<List<BitMap>> bitMaps = null;
    for (int column : columns) {
      types.add(this.dataTypes.get(column));
      values.add(this.values.get(column));
      if (this.bitMaps != null && this.bitMaps.get(column) != null) {
        if (bitMaps == null) {
          bitMaps = new ArrayList<>(columns.size());
          for (int i = 0; i < columns.size(); i++) {
            bitMaps.add(null);
          }
        }
        bitMaps.set(columns.indexOf(column), this.bitMaps.get(column));
      }
    }
    VectorTVList vectorTvList = new VectorTVList(types);
    vectorTvList.timestamps = this.timestamps;
    vectorTvList.indices = this.indices;
    vectorTvList.values = values;
    vectorTvList.bitMaps = bitMaps;
    vectorTvList.size = this.size;
    return vectorTvList;
  }

  /**
   * Get the int value at the given position in VectorTvList.
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
   * Get whether value is marked at the given position in VectorTvList.
   *
   * @param rowIndex value index inside this column
   * @param columnIndex index of the column
   * @return boolean
   */
  public boolean isValueMarked(int rowIndex, int columnIndex) {
    if (rowIndex >= size) {
      return false;
    }
    if (bitMaps == null
        || bitMaps.get(columnIndex) == null
        || bitMaps.get(columnIndex).get(rowIndex / ARRAY_SIZE) == null) {
      return false;
    }
    int arrayIndex = rowIndex / ARRAY_SIZE;
    int elementIndex = rowIndex % ARRAY_SIZE;
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
    int newSize = 0;
    minTime = Long.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      long time = getTime(i);
      if (time < lowerBound || time > upperBound) {
        set(i, newSize++);
        minTime = Math.min(time, minTime);
      }
    }
    int deletedNumber = size - newSize;
    size = newSize;
    // release primitive arrays that are empty
    int newArrayNum = newSize / ARRAY_SIZE;
    if (newSize % ARRAY_SIZE != 0) {
      newArrayNum++;
    }
    for (int releaseIdx = newArrayNum; releaseIdx < timestamps.size(); releaseIdx++) {
      releaseLastTimeArray();
      releaseLastValueArray();
    }
    return deletedNumber * getTsDataTypes().size();
  }

  // TODO: THIS METHOLD IS FOR DELETING ONE COLUMN OF A VECTOR
  public int delete(long lowerBound, long upperBound, int columnIndex) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  protected void set(int index, long timestamp, int value) {
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    indices.get(arrayIndex)[elementIndex] = value;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public VectorTVList clone() {
    VectorTVList cloneList = new VectorTVList(dataTypes);
    cloneAs(cloneList);
    for (int[] indicesArray : indices) {
      cloneList.indices.add(cloneIndex(indicesArray));
    }
    for (int i = 0; i < values.size(); i++) {
      List<Object> columnValues = values.get(i);
      for (Object valueArray : columnValues) {
        cloneList.values.get(i).add(cloneValue(dataTypes.get(i), valueArray));
      }
      // clone bitmap in columnIndex
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
    return cloneList;
  }

  private int[] cloneIndex(int[] array) {
    int[] cloneArray = new int[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  private Object cloneValue(TSDataType type, Object value) {
    switch (type) {
      case TEXT:
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
        int[] valueI = (int[]) value;
        int[] cloneI = new int[valueI.length];
        System.arraycopy(valueI, 0, cloneI, 0, valueI.length);
        return cloneI;
      case INT64:
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
  public void sort() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, size);
    }
    if (sortedIndices == null || sortedIndices.length < size) {
      sortedIndices = (int[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT32, size);
    }
    sort(0, size);
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  void clearValue() {
    if (indices != null) {
      for (int[] dataArray : indices) {
        PrimitiveArrayManager.release(dataArray);
      }
      indices.clear();
    }
    for (int i = 0; i < dataTypes.size(); i++) {
      List<Object> columnValues = values.get(i);
      if (columnValues != null) {
        for (Object dataArray : columnValues) {
          PrimitiveArrayManager.release(dataArray);
        }
        columnValues.clear();
      }
      if (bitMaps != null) {
        List<BitMap> columnBitMaps = bitMaps.get(i);
        if (columnBitMaps != null) {
          columnBitMaps.clear();
        }
      }
    }
  }

  @Override
  void clearSortedValue() {
    if (sortedIndices != null) {
      sortedIndices = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedIndices[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getValueIndex(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedIndices[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getValueIndex(src);
  }

  @Override
  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      int loV = getValueIndex(lo);
      long hiT = getTime(hi);
      int hiV = getValueIndex(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void expandValues() {
    indices.add((int[]) getPrimitiveArraysByType(TSDataType.INT32));
    for (int i = 0; i < dataTypes.size(); i++) {
      values.get(i).add(getPrimitiveArraysByType(dataTypes.get(i)));
      if (bitMaps != null && bitMaps.get(i) != null) {
        bitMaps.get(i).add(null);
      }
    }
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotIndex = getValueIndex(pos);
  }

  /**
   * Get the row index value in index column
   *
   * @param index row index
   */
  @Override
  public int getValueIndex(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return indices.get(arrayIndex)[elementIndex];
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
      if (!isValueMarked(originRowIndex, columnIndex)) {
        validRowIndex = originRowIndex;
      }
    }
    return validRowIndex;
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotIndex);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(getTime(index), (TsPrimitiveType) getVector(index));
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(time, (TsPrimitiveType) getVector(index));
  }

  @Override
  public TimeValuePair getTimeValuePairForTimeDuplicatedRows(
      List<Integer> indexList, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(time, getVector(indexList));
  }

  @Override
  protected void releaseLastValueArray() {
    PrimitiveArrayManager.release(indices.remove(indices.size() - 1));
    for (List<Object> valueList : values) {
      PrimitiveArrayManager.release(valueList.remove(valueList.size() - 1));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public void putVectors(
      long[] time, Object[] value, BitMap[] bitMaps, int[] columnOrder, int start, int end) {
    checkExpansion();
    int idx = start;

    updateMinTimeAndSorted(time, start, end);

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = size / ARRAY_SIZE;
      int elementIdx = size % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        arrayCopy(value, idx, arrayIdx, elementIdx, inputRemaining, columnOrder);
        for (int i = 0; i < inputRemaining; i++) {
          indices.get(arrayIdx)[elementIdx + i] = size;
          if (bitMaps != null) {
            for (int j = 0; j < bitMaps.length; j++) {
              if (bitMaps[columnOrder[j]] != null && bitMaps[columnOrder[j]].isMarked(idx + i)) {
                markNullValue(j, arrayIdx, elementIdx + i);
              }
            }
          }
          size++;
        }
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        arrayCopy(value, idx, arrayIdx, elementIdx, internalRemaining, columnOrder);
        for (int i = 0; i < internalRemaining; i++) {
          indices.get(arrayIdx)[elementIdx + i] = size;
          if (bitMaps != null) {
            for (int j = 0; j < bitMaps.length; j++) {
              if (bitMaps[columnOrder[j]] != null && bitMaps[columnOrder[j]].isMarked(idx + i)) {
                markNullValue(j, arrayIdx, elementIdx + i);
              }
            }
          }
          size++;
        }
        idx += internalRemaining;
        checkExpansion();
      }
    }
  }

  private void arrayCopy(
      Object[] value, int idx, int arrayIndex, int elementIndex, int remaining, int[] columnOrder) {
    for (int i = 0; i < values.size(); i++) {
      List<Object> columnValues = values.get(i);
      switch (dataTypes.get(i)) {
        case TEXT:
          Binary[] arrayT = ((Binary[]) columnValues.get(arrayIndex));
          System.arraycopy(value[columnOrder[i]], idx, arrayT, elementIndex, remaining);
          break;
        case FLOAT:
          float[] arrayF = ((float[]) columnValues.get(arrayIndex));
          System.arraycopy(value[columnOrder[i]], idx, arrayF, elementIndex, remaining);
          break;
        case INT32:
          int[] arrayI = ((int[]) columnValues.get(arrayIndex));
          System.arraycopy(value[columnOrder[i]], idx, arrayI, elementIndex, remaining);
          break;
        case INT64:
          long[] arrayL = ((long[]) columnValues.get(arrayIndex));
          System.arraycopy(value[columnOrder[i]], idx, arrayL, elementIndex, remaining);
          break;
        case DOUBLE:
          double[] arrayD = ((double[]) columnValues.get(arrayIndex));
          System.arraycopy(value[columnOrder[i]], idx, arrayD, elementIndex, remaining);
          break;
        case BOOLEAN:
          boolean[] arrayB = ((boolean[]) columnValues.get(arrayIndex));
          System.arraycopy(value[columnOrder[i]], idx, arrayB, elementIndex, remaining);
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
        columnBitMaps.add(null);
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

  /**
   * Get the single vectorTVList array size by give types.
   *
   * @param types the types in the vector
   * @return VectorTvListArrayMemSize
   */
  public static long vectorTvListArrayMemSize(TSDataType[] types) {
    long size = 0;
    // time size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // index size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * 4L;
    // value size
    for (TSDataType type : types) {
      size += (long) PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    }
    return size;
  }

  @Override
  @TestOnly
  public IPointReader getIterator() {
    return new VectorIte();
  }

  @Override
  public IPointReader getIterator(
      int floatPrecision, TSEncoding encoding, int size, List<TimeRange> deletionList) {
    return new VectorIte(floatPrecision, encoding, size, deletionList);
  }

  private class VectorIte extends Ite {

    public VectorIte() {
      super();
    }

    public VectorIte(
        int floatPrecision, TSEncoding encoding, int size, List<TimeRange> deletionList) {
      super(floatPrecision, encoding, size, deletionList);
    }

    @Override
    public boolean hasNextTimeValuePair() {
      if (hasCachedPair) {
        return true;
      }

      List<Integer> timeDuplicatedVectorRowIndexList = null;
      while (cur < iteSize) {
        long time = getTime(cur);
        if (isPointDeleted(time) || (cur + 1 < size() && (time == getTime(cur + 1)))) {
          if (timeDuplicatedVectorRowIndexList == null) {
            timeDuplicatedVectorRowIndexList = new ArrayList<>();
            timeDuplicatedVectorRowIndexList.add(getValueIndex(cur));
          }
          timeDuplicatedVectorRowIndexList.add(getValueIndex(cur + 1));
          cur++;
          continue;
        }
        TimeValuePair tvPair;
        if (timeDuplicatedVectorRowIndexList != null) {
          tvPair =
              getTimeValuePairForTimeDuplicatedRows(
                  timeDuplicatedVectorRowIndexList, time, floatPrecision, encoding);
          timeDuplicatedVectorRowIndexList = null;
        } else {
          tvPair = getTimeValuePair(cur, time, floatPrecision, encoding);
        }
        cur++;
        if (tvPair.getValue() != null) {
          cachedTimeValuePair = tvPair;
          hasCachedPair = true;
          return true;
        }
      }

      return false;
    }
  }
}
