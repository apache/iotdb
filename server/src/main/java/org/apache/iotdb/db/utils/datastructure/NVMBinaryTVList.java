package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.nvm.space.NVMSpaceMetadataManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

// TODO how to organize data
public class NVMBinaryTVList extends NVMTVList {

  // TODO
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
    int elementIndex = 0;
    minTime = minTime <= timestamp ? minTime : timestamp;
    timestamps.get(arrayIndex).setData(elementIndex, timestamp);
    values.get(arrayIndex).setData(elementIndex, value);
    size++;
    if (sorted && size > 1 && timestamp < getTime(size - 2)) {
      sorted = false;
    }
  }

  @Override
  protected void checkExpansion() {
    NVMDataSpace valueSpace = expandValues();
    NVMDataSpace timeSpace = NVMPrimitiveArrayPool
        .getInstance().getPrimitiveDataListByType(TSDataType.INT64, true);
    timestamps.add(timeSpace);
    NVMSpaceMetadataManager
        .getInstance().registerTVSpace(timeSpace, valueSpace, sgId, deviceId, measurementId);
  }

  @Override
  public Binary getBinary(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index;
    int elementIndex = 0;
    return (Binary) values.get(arrayIndex).getData(elementIndex);
  }

  @Override
  public BinaryTVList clone() {
    BinaryTVList cloneList = new BinaryTVList();
    cloneAs(cloneList);
    for (NVMDataSpace valueSpace : values) {
      cloneList.addBatchValue((Binary[]) cloneValue(valueSpace));
    }
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
    checkExpansion();
    int idx = 0;
    int length = time.length;

    for (int i = 0; i < length; i++) {
      putBinary(time[i], value[i]);
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

  public static void main(String[] args) throws StartupException {
    NVMSpaceManager.getInstance().init();

    NVMBinaryTVList tvList = new NVMBinaryTVList("sg", "d0", "s0");
    int size = 5000;
    for (int i = 0; i < size; i++) {
      String v = String.valueOf(size - i);
      tvList.putBinary(i, Binary.valueOf(v));
    }

    tvList.sort();
  }
}
