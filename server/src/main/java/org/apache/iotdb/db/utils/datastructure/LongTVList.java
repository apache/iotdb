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
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class LongTVList extends TVList {

  // list of primitive array, add 1 when expanded -> long primitive array
  // index relation: arrayIndex -> elementIndex
  private List<long[]> values;

  private long[][] sortedValues;

  private long pivotValue;

  LongTVList() {
    super();
    values = new ArrayList<>();
  }

  @Override
  public void putLong(long timestamp, long value) {
    checkExpansion();
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;
    minTime = Math.min(minTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    rowCount++;
    if (sorted && rowCount > 1 && timestamp < getTime(rowCount - 2)) {
      sorted = false;
    }
  }

  @Override
  public long getLong(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return values.get(arrayIndex)[elementIndex];
  }

  protected void set(int index, long timestamp, long value) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  public LongTVList clone() {
    LongTVList cloneList = new LongTVList();
    cloneAs(cloneList);
    for (long[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  private long[] cloneValue(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null || sortedTimestamps.length < rowCount) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, rowCount);
    }
    if (sortedValues == null || sortedValues.length < rowCount) {
      sortedValues =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, rowCount);
    }
    sort(0, rowCount);
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  void clearValue() {
    if (values != null) {
      for (long[] dataArray : values) {
        PrimitiveArrayManager.release(dataArray);
      }
      values.clear();
    }
  }

  @Override
  void clearSortedValue() {
    if (sortedValues != null) {
      sortedValues = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedValues[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    long srcV = getLong(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedValues[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getLong(src);
  }

  @Override
  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      long loV = getLong(lo);
      long hiT = getTime(hi);
      long hiV = getLong(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void expandValues() {
    values.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getLong(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(
        getTime(index), TsPrimitiveType.getByType(TSDataType.INT64, getLong(index)));
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, getLong(index)));
  }

  @Override
  protected void writeValidValuesIntoTsBlock(
      TsBlockBuilder builder,
      int floatPrecision,
      TSEncoding encoding,
      List<TimeRange> deletionList) {
    Integer deleteCursor = 0;
    for (int i = 0; i < rowCount; i++) {
      if (!isPointDeleted(getTime(i), deletionList, deleteCursor)
          && (i == rowCount - 1 || getTime(i) != getTime(i + 1))) {
        builder.getTimeColumnBuilder().writeLong(getTime(i));
        builder.getColumnBuilder(0).writeLong(getLong(i));
        builder.declarePosition();
      }
    }
  }

  @Override
  protected void releaseLastValueArray() {
    PrimitiveArrayManager.release(values.remove(values.size() - 1));
  }

  @Override
  public void putLongs(long[] time, long[] value, BitMap bitMap, int start, int end) {
    checkExpansion();

    int idx = start;
    // constraint: time.length + timeIdxOffset == value.length
    int timeIdxOffset = 0;
    if (bitMap != null && !bitMap.isAllUnmarked()) {
      // time array is a reference, should clone necessary time values
      long[] clonedTime = new long[end - start];
      System.arraycopy(time, start, clonedTime, 0, end - start);
      time = clonedTime;
      timeIdxOffset = start;
      // drop null at the end of value array
      int nullCnt =
          dropNullValThenUpdateMinTimeAndSorted(time, value, bitMap, start, end, timeIdxOffset);
      end -= nullCnt;
    } else {
      updateMinTimeAndSorted(time, start, end);
    }

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = rowCount / ARRAY_SIZE;
      int elementIdx = rowCount % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, inputRemaining);
        rowCount += inputRemaining;
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, internalRemaining);
        idx += internalRemaining;
        rowCount += internalRemaining;
        checkExpansion();
      }
    }
  }

  // move null values to the end of time array and value array, then return number of null values
  int dropNullValThenUpdateMinTimeAndSorted(
      long[] time, long[] values, BitMap bitMap, int start, int end, int tIdxOffset) {
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;

    int nullCnt = 0;
    for (int vIdx = start; vIdx < end; vIdx++) {
      if (bitMap.isMarked(vIdx)) {
        nullCnt++;
        continue;
      }
      // move value ahead to replace null
      int tIdx = vIdx - tIdxOffset;
      if (nullCnt != 0) {
        time[tIdx - nullCnt] = time[tIdx];
        values[vIdx - nullCnt] = values[vIdx];
      }
      // update minTime and sorted
      tIdx = tIdx - nullCnt;
      inPutMinTime = Math.min(inPutMinTime, time[tIdx]);
      if (inputSorted && tIdx > 0 && time[tIdx - 1] > time[tIdx]) {
        inputSorted = false;
      }
    }
    minTime = Math.min(inPutMinTime, minTime);
    sorted = sorted && inputSorted && (rowCount == 0 || inPutMinTime >= getTime(rowCount - 1));
    return nullCnt;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.INT64;
  }

  @Override
  public int serializedSize() {
    return Byte.BYTES + Integer.BYTES + rowCount * 2 * Long.BYTES;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(TSDataType.INT64, buffer);
    buffer.putInt(rowCount);
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      buffer.putLong(getTime(rowIdx));
      buffer.putLong(getLong(rowIdx));
    }
  }

  public static LongTVList deserialize(DataInputStream stream) throws IOException {
    LongTVList tvList = new LongTVList();
    int rowCount = stream.readInt();
    long[] times = new long[rowCount];
    long[] values = new long[rowCount];
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      times[rowIdx] = stream.readLong();
      values[rowIdx] = stream.readLong();
    }
    tvList.putLongs(times, values, null, 0, rowCount);
    return tvList;
  }
}
