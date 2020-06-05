package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool;
import org.apache.iotdb.db.nvm.space.NVMBinaryDataSpace;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.nvm.space.NVMSpaceMetadataManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

// TODO how to organize data
public class NVMBinaryTVList extends NVMTVList {

  private Binary[][] sortedValues;
  private Binary[][] tempValuesForSort;

  NVMBinaryTVList(String sgId, String deviceId, String measurementId) {
    super(sgId, deviceId, measurementId);
    dataType = TSDataType.TEXT;
  }

  @Override
  public void putBinary(long timestamp, Binary value) {
    checkExpansion();
    int arrayIndex = size;
    minTime = minTime <= timestamp ? minTime : timestamp;
    timestamps.get(arrayIndex).setData(0, timestamp);
    ((NVMBinaryDataSpace)values.get(arrayIndex)).appendData(value);
    size++;
    if (sorted && size > 1 && timestamp < getTime(size - 2)) {
      sorted = false;
    }
  }

  @Override
  protected void checkExpansion() {
    NVMBinaryDataSpace valueSpace = (NVMBinaryDataSpace) expandValues();
    valueSpace.reset();
    NVMDataSpace timeSpace = NVMPrimitiveArrayPool
        .getInstance().getPrimitiveDataListByType(TSDataType.INT64, true);
    timestamps.add(timeSpace);
    NVMSpaceMetadataManager
        .getInstance().registerTVSpace(timeSpace, valueSpace, sgId, deviceId, measurementId);
  }

  @Override
  public long getTime(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return (long) timestamps.get(index).getData(0);
  }

  @Override
  public Binary getBinary(int index) {
    return (Binary) getValue(index);
  }

  @Override
  public Object getValue(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return values.get(index).getData(0);
  }

  @Override
  protected void set(int index, long timestamp, Object value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    timestamps.get(index).setData(0, timestamp);
    values.get(index).setData(0, value);
  }

  @Override
  public BinaryTVList clone() {
    BinaryTVList cloneList = new BinaryTVList();

    long[] cloneTimestamps = new long[size];
    Binary[] cloneValues = new Binary[size];
    for (int i = 0; i < size; i++) {
      cloneTimestamps[i] = timestamps.get(i).getLong(0);
      cloneValues[i] = (Binary) values.get(i).getData(0);
    }
    cloneList.putBinaries(cloneTimestamps, cloneValues);
    return cloneList;
  }

  @Override
  protected void initTempArrays() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
      tempTimestampsForSort = (long[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(TSDataType.INT64, size);
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues = (Binary[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
      tempValuesForSort = (Binary[][]) PrimitiveArrayPool
          .getInstance().getDataListsByType(dataType, size);
    }
  }

  @Override
  protected void copyTVToTempArrays() {
    int arrayIndex = 0;
    int elementIndex = 0;
    for (int i = 0; i < size; i++) {
      long time = (long) timestamps.get(i).getData(0);
      Binary value = (Binary) values.get(i).getData(0);
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
      Binary value = tempValuesForSort[arrayIndex][elementIndex];

      timestamps.get(i).setData(0, time);
      values.get(i).setData(0, value);

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
      for (Binary[] dataArray : sortedValues) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      sortedValues = null;
    }

    if (tempValuesForSort != null) {
      for (Binary[] dataArray : tempValuesForSort) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      tempValuesForSort = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    setForSort(dest, sortedTimestamps[src/ARRAY_SIZE][src%ARRAY_SIZE], sortedValues[src/ARRAY_SIZE][src%ARRAY_SIZE]);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    Binary srcV = getBinary(src);
    set(dest, srcT, srcV);
  }

  @Override
  protected void setValueForSort(int arrayIndex, int elementIndex, Object value) {
    tempValuesForSort[arrayIndex][elementIndex] = (Binary) value;
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
    sortedValues[dest/ARRAY_SIZE][dest%ARRAY_SIZE] = (Binary) getValueForSort(src);
  }
  
  @Override
  public void putBinaries(long[] time, Binary[] value) {
    int length = time.length;

    for (int i = 0; i < length; i++) {
      putBinary(time[i], value[i]);
    }
  }

  @Override
  public void putBinaries(long[] time, Binary[] value, int start, int end) {
    for (int i = start; i < end; i++) {
      putBinary(time[i], value[i]);
    }
  }
}
