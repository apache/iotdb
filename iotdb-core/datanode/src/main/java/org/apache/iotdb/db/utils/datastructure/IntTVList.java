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

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
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

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.*;

public abstract class IntTVList extends TVList {
  // list of primitive array, add 1 when expanded -> int primitive array
  // index relation: arrayIndex -> elementIndex
  protected List<int[]> values;

  IntTVList() {
    super();
    values = new ArrayList<>();
  }

  public static IntTVList newList() {
    switch (TVLIST_SORT_ALGORITHM) {
      case QUICK:
        return new QuickIntTVList();
      case BACKWARD:
        return new BackIntTVList();
      default:
        return new TimIntTVList();
    }
  }

  @Override
  public IntTVList clone() {
    IntTVList cloneList = IntTVList.newList();
    cloneAs(cloneList);
    for (int[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  private int[] cloneValue(int[] array) {
    int[] cloneArray = new int[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  @Override
  public void putInt(long timestamp, int value) {
    checkExpansion();
    if (MEMTABLE_TOPK_SIZE == 0) {
      int arrayIndex = rowCount / ARRAY_SIZE;
      int elementIndex = rowCount % ARRAY_SIZE;
      maxTime = Math.max(maxTime, timestamp);
      topKTime = Math.max(topKTime, timestamp);
      timestamps.get(arrayIndex)[elementIndex] = timestamp;
      values.get(arrayIndex)[elementIndex] = value;
      rowCount++;
      if (sorted && rowCount > 1 && timestamp < getTime(rowCount - 2)) {
        sorted = false;
      }
    } else {
      int arrayIndex;
      int elementIndex;
      int waitlen = Math.min(rowCount, MEMTABLE_TOPK_SIZE);
      int tempRowCount = rowCount - 1; // 记录向前比较的截止位置
      while (waitlen > 0) {
        waitlen--;
        arrayIndex = tempRowCount / ARRAY_SIZE;
        elementIndex = tempRowCount % ARRAY_SIZE;
        if (timestamps.get(arrayIndex)[elementIndex] > timestamp) {
          tempRowCount--;
          continue;
        }
        break;
      }
      arrayIndex = rowCount / ARRAY_SIZE;
      elementIndex = rowCount % ARRAY_SIZE;
      int arrayIndexLeft = (rowCount - 1) / ARRAY_SIZE;
      int elementIndexLeft = (rowCount - 1) % ARRAY_SIZE;
      for (int i = rowCount - 1; i > tempRowCount; i--) {
        timestamps.get(arrayIndex)[elementIndex] = timestamps.get(arrayIndexLeft)[elementIndexLeft];
        values.get(arrayIndex)[elementIndex] = values.get(arrayIndexLeft)[elementIndexLeft];
        arrayIndex = arrayIndexLeft;
        elementIndex = elementIndexLeft;
        arrayIndexLeft = (i - 1) / ARRAY_SIZE;
        elementIndexLeft = (i - 1) % ARRAY_SIZE;
        sortCount++;
      }
      arrayIndex = (tempRowCount + 1) / ARRAY_SIZE;
      elementIndex = (tempRowCount + 1) % ARRAY_SIZE;
      maxTime = Math.max(maxTime, timestamp);
      timestamps.get(arrayIndex)[elementIndex] = timestamp;
      values.get(arrayIndex)[elementIndex] = value;
      if (rowCount > MEMTABLE_TOPK_SIZE) {
        arrayIndex = (rowCount - MEMTABLE_TOPK_SIZE - 1) / ARRAY_SIZE;
        elementIndex = (rowCount - MEMTABLE_TOPK_SIZE - 1) % ARRAY_SIZE;
        topKTime = Math.max(topKTime, timestamps.get(arrayIndex)[elementIndex]);
      }
      rowCount++;
      if (sorted && rowCount > 1 && waitlen == 0) {
        sorted = false;
      }
    }
  }

  @Override
  public int getInt(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return values.get(arrayIndex)[elementIndex];
  }

  protected void set(int index, long timestamp, int value) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  void clearValue() {
    if (values != null) {
      for (int[] dataArray : values) {
        PrimitiveArrayManager.release(dataArray);
      }
      values.clear();
    }
  }

  @Override
  protected void expandValues() {
    values.add((int[]) getPrimitiveArraysByType(TSDataType.INT32));
  }

  @Override
  public TVList divide() throws TopkDivideMemoryNotEnoughException {
    if (MEMTABLE_TOPK_SIZE > rowCount || MEMTABLE_TOPK_SIZE < ARRAY_SIZE) {
      throw new TopkDivideMemoryNotEnoughException(
          String.format(
              "WARNING: MEMTABLE_TOPK_SIZE %d is bigger than "
                  + "the TVList's row count %d, "
                  + "or is smaller than ARRAY_SIZE %d",
              MEMTABLE_TOPK_SIZE, rowCount, ARRAY_SIZE));
    }
    IntTVList topkTVList = IntTVList.newList();
    int truncatedIndex = rowCount - MEMTABLE_TOPK_SIZE;
    int truncatedArrayIndex =
        truncatedIndex / ARRAY_SIZE; // no matter truncatedIndex in or not in the block
    truncatedIndex = truncatedArrayIndex * ARRAY_SIZE;
    for (int i = truncatedArrayIndex; i < timestamps.size(); i++) {
      topkTVList.timestamps.add(timestamps.get(i));
      topkTVList.values.add(values.get(i));
    }
    for (int i = timestamps.size() - 1; i >= truncatedArrayIndex; i--) {
      timestamps.remove(timestamps.size() - 1);
      values.remove(values.size() - 1);
    }
    topkTVList.rowCount = rowCount - truncatedIndex;
    topkTVList.sorted = true;
    topkTVList.topKTime = topkTVList.getTime(0);
    rowCount = truncatedIndex;
    topKTime = topkTVList.getTopKTime();
    return topkTVList;
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(
        getTime(index), TsPrimitiveType.getByType(TSDataType.INT32, getInt(index)));
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT32, getInt(index)));
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
        builder.getColumnBuilder(0).writeInt(getInt(i));
        builder.declarePosition();
      }
    }
  }

  @Override
  protected void releaseLastValueArray() {
    PrimitiveArrayManager.release(values.remove(values.size() - 1));
  }

  @Override
  public void putInts(long[] time, int[] value, BitMap bitMap, int start, int end) {
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
          dropNullValThenUpdateMaxTimeAndSorted(time, value, bitMap, start, end, timeIdxOffset);
      end -= nullCnt;
    } else {
      updateMaxTimeAndSorted(time, start, end);
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
    if (MEMTABLE_TOPK_SIZE != 0) {
      sort(Math.max(0, start - MEMTABLE_TOPK_SIZE), rowCount);
      int index = Math.max(0, rowCount - MEMTABLE_TOPK_SIZE - 1);
      topKTime = Math.max(topKTime, getTime(index));
    }
  }

  // move null values to the end of time array and value array, then return number of null values
  int dropNullValThenUpdateMaxTimeAndSorted(
      long[] time, int[] values, BitMap bitMap, int start, int end, int tIdxOffset) {
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
      // update maxTime and sorted
      tIdx = tIdx - nullCnt;
      inPutMinTime = Math.min(inPutMinTime, time[tIdx]);
      maxTime = Math.max(maxTime, time[tIdx]);
      if (inputSorted && tIdx > 0 && time[tIdx - 1] > time[tIdx]) {
        inputSorted = false;
      }
    }

    sorted = sorted && inputSorted && (rowCount == 0 || inPutMinTime >= getTime(rowCount - 1));
    return nullCnt;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.INT32;
  }

  @Override
  public int serializedSize() {
    return Byte.BYTES + Integer.BYTES + rowCount * (Long.BYTES + Integer.BYTES);
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(TSDataType.INT32, buffer);
    buffer.putInt(rowCount);
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      buffer.putLong(getTime(rowIdx));
      buffer.putInt(getInt(rowIdx));
    }
  }

  public static IntTVList deserialize(DataInputStream stream) throws IOException {
    IntTVList tvList = IntTVList.newList();
    int rowCount = stream.readInt();
    long[] times = new long[rowCount];
    int[] values = new int[rowCount];
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      times[rowIdx] = stream.readLong();
      values[rowIdx] = stream.readInt();
    }
    tvList.putInts(times, values, null, 0, rowCount);
    return tvList;
  }
}
