package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMDoubleTVList extends NVMTVList {

  // TODO
  private double[][] sortedValues;
  private double[][] tempValuesForSort;

  NVMDoubleTVList(String sgId, String deviceId, String measurementId) {
    super(sgId, deviceId, measurementId);
    dataType = TSDataType.DOUBLE;
  }

  @Override
  public void putDouble(long timestamp, double value) {
    checkExpansion();

    int arrayIndex = size / ARRAY_SIZE;
    int elementIndex = size % ARRAY_SIZE;
    minTime = minTime <= timestamp ? minTime : timestamp;
    timestamps.get(arrayIndex).setData(elementIndex, timestamp);
    values.get(arrayIndex).setData(elementIndex, value);
    size++;
    if (sorted && size > 1 && timestamp < getTime(size - 2)) {
      sorted = false;
    }
  }

  @Override
  public double getDouble(int index) {
    return (double) getValue(index);
  }

  @Override
  public DoubleTVList clone() {
    DoubleTVList cloneList = new DoubleTVList();
    cloneAs(cloneList);
    for (NVMDataSpace valueSpace : values) {
      cloneList.addBatchValue((double[]) cloneValue(valueSpace));
    }
    return cloneList;
  }

  @Override
  protected void initSortedAndTempArrays() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
      tempTimestampsForSort = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues = (double[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
      tempValuesForSort = (double[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
    }
  }

  @Override
  protected void copyTVToTempArrays() {
    int arrayIndex = 0;
    int elementIndex = 0;
    for (int i = 0; i < size; i++) {
      long time = (long) timestamps.get(arrayIndex).getData(elementIndex);
      double value = (double) values.get(arrayIndex).getData(elementIndex);
      tempTimestampsForSort[arrayIndex][elementIndex] = time;
      tempValuesForSort[arrayIndex][elementIndex] = value;

      elementIndex++;
      if (elementIndex == ARRAY_SIZE) {
        elementIndex = 0;
        arrayIndex++;
      }
    }
  }

  @Override
  protected void copyTVFromTempArrays() {
    int arrayIndex = 0;
    int elementIndex = 0;
    for (int i = 0; i < size; i++) {
      long time = tempTimestampsForSort[arrayIndex][elementIndex];
      double value = tempValuesForSort[arrayIndex][elementIndex];

      timestamps.get(arrayIndex).setData(elementIndex, time);
      values.get(arrayIndex).setData(elementIndex, value);

      elementIndex++;
      if (elementIndex == ARRAY_SIZE) {
        elementIndex = 0;
        arrayIndex++;
      }
    }
  }

  @Override
  protected void clearSortedValue() {
    if (sortedValues != null) {
      for (double[] dataArray : sortedValues) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      sortedValues = null;
    }

    if (tempValuesForSort != null) {
      for (double[] dataArray : tempValuesForSort) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      tempValuesForSort = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    setForSort(dest, sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedValues[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    double srcV = getDouble(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setValueForSort(int arrayIndex, int elementIndex, Object value) {
    tempValuesForSort[arrayIndex][elementIndex] = (double) value;
  }

  @Override
  protected Object getValueForSort(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return tempValuesForSort[arrayIndex][elementIndex];
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest/ARRAY_SIZE][dest% ARRAY_SIZE] = getTimeForSort(src);
    sortedValues[dest/ARRAY_SIZE][dest%ARRAY_SIZE] = (double) getValueForSort(src);
  }

  @Override
  public void putDoubles(long[] time, double[] value) {
    int length = time.length;

    for (int i = 0; i < length; i++) {
      putDouble(time[i], value[i]);
    }
  }

  @Override
  public void putDoubles(long[] time, double[] value, int start, int end) {
    for (int i = start; i < end; i++) {
      putDouble(time[i], value[i]);
    }
  }
}
