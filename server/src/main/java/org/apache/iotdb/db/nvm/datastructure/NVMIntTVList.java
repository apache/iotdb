package org.apache.iotdb.db.nvm.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSpace;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMIntTVList extends NVMTVList {

  // TODO
  private int[][] sortedValues;

  private int pivotValue;

  public NVMIntTVList() {
    super();
    dataType = TSDataType.INT32;
  }

  @Override
  public void putInt(long timestamp, int value) {
    checkExpansion();
    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    minTime = minTime <= timestamp ? minTime : timestamp;
    timestamps.get(arrayIndex).set(elementIndex, timestamp);
    values.get(arrayIndex).set(elementIndex, value);
    size++;
    if (sorted && size > 1 && timestamp < getTime(size - 2)) {
      sorted = false;
    }
  }

  @Override
  public int getInt(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return (int) values.get(arrayIndex).get(elementIndex);
  }

  @Override
  public NVMIntTVList clone() {
    NVMIntTVList cloneList = new NVMIntTVList();
    cloneAs(cloneList);
    for (NVMSpace valueSpace : values) {
      cloneList.values.add(cloneValue(valueSpace));
    }
    return cloneList;
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues = (int[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
    }
    sort(0, size);
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  protected void clearSortedValue() {
    if (sortedValues != null) {
      for (int[] dataArray : sortedValues) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      sortedValues = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(dest, sortedTimestamps[src/ARRAY_SIZE][src%ARRAY_SIZE], sortedValues[src/ARRAY_SIZE][src%ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getInt(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest/ARRAY_SIZE][dest% ARRAY_SIZE] = getTime(src);
    sortedValues[dest/ARRAY_SIZE][dest%ARRAY_SIZE] = getInt(src);
  }

  @Override
  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      int loV = getInt(lo);
      long hiT = getTime(hi);
      int hiV = getInt(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getInt(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

  @Override
  public void putInts(long[] time, int[] value) {
    checkExpansion();
    int idx = 0;
    int length = time.length;

    for (int i = 0; i < length; i++) {
      putInt(time[i], value[i]);
    }

//    updateMinTimeAndSorted(time);
//
//    while (idx < length) {
//      int inputRemaining = length - idx;
//      int arrayIdx = size / ARRAY_SIZE;
//      int elementIdx = size % ARRAY_SIZE;
//      int internalRemaining  = ARRAY_SIZE - elementIdx;
//      if (internalRemaining >= inputRemaining) {
//        // the remaining inputs can fit the last array, copy all remaining inputs into last array
//        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, inputRemaining);
//        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, inputRemaining);
//        size += inputRemaining;
//        break;
//      } else {
//        // the remaining inputs cannot fit the last array, fill the last array and create a new
//        // one and enter the next loop
//        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, internalRemaining);
//        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, internalRemaining);
//        idx += internalRemaining;
//        size += internalRemaining;
//        checkExpansion();
//      }
//    }
  }
}
