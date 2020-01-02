package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMLongTVList extends NVMTVList {

  // TODO
  private long[][] sortedValues;

  private long pivotValue;

  NVMLongTVList(String sgId, String deviceId, String measurementId) {
    super(sgId, deviceId, measurementId);
    dataType = TSDataType.INT64;
  }

  @Override
  public void putLong(long timestamp, long value) {
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
  public long getLong(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return (long) values.get(arrayIndex).get(elementIndex);
  }

  @Override
  public LongTVList clone() {
    LongTVList cloneList = new LongTVList();
    cloneAs(cloneList);
    for (NVMDataSpace valueSpace : values) {
      cloneList.addBatchValue((long[]) cloneValue(valueSpace));
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
      sortedValues = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
    }
    sort(0, size);
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  protected void clearSortedValue() {
    if (sortedValues != null) {
      for (long[] dataArray : sortedValues) {
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
    long srcV = getLong(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest/ARRAY_SIZE][dest% ARRAY_SIZE] = getTime(src);
    sortedValues[dest/ARRAY_SIZE][dest%ARRAY_SIZE] = getLong(src);
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
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getLong(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

  @Override
  public void putLongs(long[] time, long[] value) {
    checkExpansion();
    int idx = 0;
    int length = time.length;

    for (int i = 0; i < length; i++) {
      putLong(time[i], value[i]);
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

