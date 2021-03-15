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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class VectorTVList extends TVList {

  private List<TSDataType> dataTypes;

  private List<List<Object>> values;

  private List<int[]> indices;

  private int[][] sortedIndices;

  private int pivotIndex;

  VectorTVList(List<TSDataType> types) {
    super();
    indices = new ArrayList<>();
    dataTypes = types;
    values = new ArrayList<>();
    for (int i = 0; i < types.size(); i++) {
      values.add(new ArrayList<>());
    }
  }

  @Override
  public void putVector(long timestamp, Object[] value) {
    checkExpansion();
    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    minTime = Math.min(minTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    for (int i = 0; i < values.size(); i++) {
      Object columnValue = value[i];
      List<Object> columnValues = values.get(i);
      switch (dataTypes.get(i)) {
        case TEXT:
          ((Binary[]) columnValues.get(arrayIndex))[elementIndex] = ((Binary[]) columnValue)[0];
          break;
        case FLOAT:
          ((float[]) columnValues.get(arrayIndex))[elementIndex] = ((float[]) columnValue)[0];
          break;
        case INT32:
          ((int[]) columnValues.get(arrayIndex))[elementIndex] = ((int[]) columnValue)[0];
          break;
        case INT64:
          ((long[]) columnValues.get(arrayIndex))[elementIndex] = ((long[]) columnValue)[0];
          break;
        case DOUBLE:
          ((double[]) columnValues.get(arrayIndex))[elementIndex] = ((double[]) columnValue)[0];
          break;
        case BOOLEAN:
          ((boolean[]) columnValues.get(arrayIndex))[elementIndex] = ((boolean[]) columnValue)[0];
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
    return getVectorByValueIndex(valueIndex);
  }

  private Object getVectorByValueIndex(int valueIndex) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    TsPrimitiveType[] vector = new TsPrimitiveType[values.size()];
    for (int i = 0; i < values.size(); i++) {
      List<Object> columnValues = values.get(i);
      switch (dataTypes.get(i)) {
        case TEXT:
          vector[i] =
              TsPrimitiveType.getByType(
                  dataTypes.get(i), ((Binary[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case FLOAT:
          vector[i] =
              TsPrimitiveType.getByType(
                  dataTypes.get(i), ((float[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case INT32:
          vector[i] =
              TsPrimitiveType.getByType(
                  dataTypes.get(i), ((int[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case INT64:
          vector[i] =
              TsPrimitiveType.getByType(
                  dataTypes.get(i), ((long[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case DOUBLE:
          vector[i] =
              TsPrimitiveType.getByType(
                  dataTypes.get(i), ((double[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        case BOOLEAN:
          vector[i] =
              TsPrimitiveType.getByType(
                  dataTypes.get(i), ((boolean[]) columnValues.get(arrayIndex))[elementIndex]);
          break;
        default:
          break;
      }
    }
    // For query one column in vectorTVList
    if (vector.length == 1) {
      return vector[0];
    }
    return TsPrimitiveType.getByType(TSDataType.VECTOR, vector);
  }

  @Override
  public TVList getTVListByColumnIndex(int column) {
    VectorTVList vectorTVList = new VectorTVList(Collections.singletonList(dataTypes.get(column)));
    vectorTVList.timestamps = this.timestamps;
    vectorTVList.indices = this.indices;
    vectorTVList.values = Collections.singletonList(this.values.get(column));
    return vectorTVList;
  }

  public int getInt(int index, int column) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getIntByValueIndex(valueIndex, column);
  }

  public int getIntByValueIndex(int valueIndex, int column) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(column);
    return ((int[]) columnValues.get(arrayIndex))[elementIndex];
  }

  public long getLong(int index, int column) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getLongByValueIndex(valueIndex, column);
  }

  public long getLongByValueIndex(int valueIndex, int column) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(column);
    return ((long[]) columnValues.get(arrayIndex))[elementIndex];
  }

  public float getFloat(int index, int column) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getFloatByValueIndex(valueIndex, column);
  }

  public float getFloatByValueIndex(int valueIndex, int column) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(column);
    return ((float[]) columnValues.get(arrayIndex))[elementIndex];
  }

  public double getDouble(int index, int column) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getDoubleByValueIndex(valueIndex, column);
  }

  public double getDoubleByValueIndex(int valueIndex, int column) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(column);
    return ((double[]) columnValues.get(arrayIndex))[elementIndex];
  }

  public Binary getBinary(int index, int column) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getBinaryByValueIndex(valueIndex, column);
  }

  public Binary getBinaryByValueIndex(int valueIndex, int column) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(column);
    return ((Binary[]) columnValues.get(arrayIndex))[elementIndex];
  }

  public boolean getBoolean(int index, int column) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    int valueIndex = indices.get(arrayIndex)[elementIndex];
    return getBooleanByValueIndex(valueIndex, column);
  }

  public boolean getBooleanByValueIndex(int valueIndex, int column) {
    if (valueIndex >= size) {
      throw new ArrayIndexOutOfBoundsException(valueIndex);
    }
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    List<Object> columnValues = values.get(column);
    return ((boolean[]) columnValues.get(arrayIndex))[elementIndex];
  }

  public List<List<Object>> getValues() {
    return values;
  }

  public List<TSDataType> getTsDataTypes() {
    return dataTypes;
  }

  public List<int[]> getIndices() {
    return indices;
  }

  protected void set(int index, long timestamp, int valueIndex) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    indices.get(arrayIndex)[elementIndex] = valueIndex;
  }

  @Override
  public VectorTVList clone() {
    VectorTVList cloneList = new VectorTVList(dataTypes);
    cloneAs(cloneList);
    for (int i = 0; i < values.size(); i++) {
      List<Object> columnValues = values.get(i);
      for (Object valueArray : columnValues) {
        cloneList.values.get(i).add(cloneValue(dataTypes.get(i), valueArray));
      }
    }
    return cloneList;
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
    }
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotIndex = getValueIndex(pos);
  }

  public int getValueIndex(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return indices.get(arrayIndex)[elementIndex];
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
    return new TimeValuePair(getTime(index), (TsPrimitiveType) getVector(index));
  }

  @Override
  protected void releaseLastValueArray() {
    PrimitiveArrayManager.release(indices.remove(indices.size() - 1));
  }

  @Override
  public void putVectors(long[] time, Object[] value, int start, int end) {
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
        arrayCopy(value, idx, arrayIdx, elementIdx, inputRemaining);
        for (int i = 0; i < inputRemaining; i++) {
          indices.get(arrayIdx)[elementIdx + i] = size;
          size++;
        }
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        arrayCopy(value, idx, arrayIdx, elementIdx, internalRemaining);
        idx += internalRemaining;
        for (int i = 0; i < internalRemaining; i++) {
          indices.get(arrayIdx)[elementIdx + i] = size;
          size++;
        }
        checkExpansion();
      }
    }
  }

  private void arrayCopy(Object[] value, int idx, int arrayIndex, int elementIndex, int remaining) {
    for (int i = 0; i < values.size(); i++) {
      List<Object> columnValues = values.get(i);
      switch (dataTypes.get(i)) {
        case TEXT:
          Binary[] arrayT = ((Binary[]) columnValues.get(arrayIndex));
          System.arraycopy((Binary[]) value[i], idx, arrayT, elementIndex, remaining);
          break;
        case FLOAT:
          float[] arrayF = ((float[]) columnValues.get(arrayIndex));
          System.arraycopy((float[]) value[i], idx, arrayF, elementIndex, remaining);
          break;
        case INT32:
          int[] arrayI = ((int[]) columnValues.get(arrayIndex));
          System.arraycopy((int[]) value[i], idx, arrayI, elementIndex, remaining);
          break;
        case INT64:
          long[] arrayL = ((long[]) columnValues.get(arrayIndex));
          System.arraycopy((long[]) value[i], idx, arrayL, elementIndex, remaining);
          break;
        case DOUBLE:
          double[] arrayD = ((double[]) columnValues.get(arrayIndex));
          System.arraycopy((double[]) value[i], idx, arrayD, elementIndex, remaining);
          break;
        case BOOLEAN:
          boolean[] arrayB = ((boolean[]) columnValues.get(arrayIndex));
          System.arraycopy((boolean[]) value[i], idx, arrayB, elementIndex, remaining);
          break;
        default:
          break;
      }
    }
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.VECTOR;
  }
}
