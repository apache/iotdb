package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.nvm.PerfMonitor;
import org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceMetadataManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class NVMTVList extends AbstractTVList {

  protected String sgId;
  protected String deviceId;
  protected String measurementId;
  protected List<NVMDataSpace> timestamps;
  protected List<NVMDataSpace> values;
  protected TSDataType dataType;

  protected long[][] tempTimestampsForSort;
  protected Object pivotValue;

  public NVMTVList(String sgId, String deviceId, String measurementId) {
    this.sgId = sgId;
    this.deviceId = deviceId;
    this.measurementId = measurementId;
    timestamps = new ArrayList<>();
    values = new ArrayList<>();
    size = 0;
    minTime = Long.MIN_VALUE;
  }

  @Override
  public long getTime(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return (long) timestamps.get(arrayIndex).getData(elementIndex);
  }

  protected void set(int index, long timestamp, Object value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex).setData(elementIndex, timestamp);
    values.get(arrayIndex).setData(elementIndex, value);
  }

  protected Object cloneValue(NVMDataSpace valueSpace) {
    return valueSpace.toArray();
  }

  @Override
  public void clear() {
    size = 0;
    timeOffset = Long.MIN_VALUE;
    sorted = true;
    minTime = Long.MIN_VALUE;

    for (int i = 0; i < timestamps.size(); i++) {
      NVMSpaceMetadataManager.getInstance().unregisterTVSpace(timestamps.get(i), values.get(i));
    }
    clearTime();
    clearSortedTime();
    clearValue();
    clearSortedValue();
  }

  @Override
  protected void clearValue() {
    if (values != null) {
      for (NVMDataSpace valueSpace : values) {
        NVMPrimitiveArrayPool.getInstance().release(valueSpace, dataType);
      }
      values.clear();
    }
  }

  @Override
  protected NVMDataSpace expandValues() {
    NVMDataSpace dataSpace = NVMPrimitiveArrayPool
        .getInstance().getPrimitiveDataListByType(dataType, false);
    values.add(dataSpace);
    return dataSpace;
  }

  @Override
  protected void releaseLastTimeArray() {
    NVMPrimitiveArrayPool.getInstance().release(timestamps.remove(timestamps.size() - 1), TSDataType.INT64);
  }

  @Override
  protected void releaseLastValueArray() {
    NVMPrimitiveArrayPool.getInstance().release(values.remove(values.size() - 1), dataType);
  }

  @Override
  public void delete(long upperBound) {
    int newSize = 0;
    minTime = Long.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      long time = getTime(i);
      if (time > upperBound) {
        set(i, newSize++);
        minTime = time < minTime ? time : minTime;
      }
    }
    size = newSize;
    // release primitive arrays that are empty
    int newArrayNum = newSize / ARRAY_SIZE;
    if (newSize % ARRAY_SIZE != 0) {
      newArrayNum ++;
    }
    for (int releaseIdx = newArrayNum; releaseIdx < timestamps.size(); releaseIdx++) {
      releaseLastTimeArray();
      releaseLastValueArray();
    }
  }

  @Override
  protected void cloneAs(AbstractTVList abstractCloneList) {
    TVList cloneList = (TVList) abstractCloneList;
    for (NVMDataSpace timeSpace : timestamps) {
      cloneList.timestamps.add((long[]) cloneTime(timeSpace));
    }
    cloneList.size = size;
    cloneList.sorted = sorted;
    cloneList.minTime = minTime;
  }

  @Override
  protected void clearTime() {
    if (timestamps != null) {
      for (NVMDataSpace timeSpace : timestamps) {
        NVMPrimitiveArrayPool.getInstance().release(timeSpace, TSDataType.INT64);
      }
      timestamps.clear();
    }
  }

  @Override
  protected void clearSortedTime() {
    if (sortedTimestamps != null) {
      for (long[] dataArray : sortedTimestamps) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      sortedTimestamps = null;
    }

    if (tempTimestampsForSort != null) {
      for (long[] dataArray : tempTimestampsForSort) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      tempTimestampsForSort = null;
    }
  }

  @Override
  protected void checkExpansion() {
    if ((size % ARRAY_SIZE) == 0) {
      NVMDataSpace valueSpace = expandValues();
      NVMDataSpace timeSpace = NVMPrimitiveArrayPool.getInstance().getPrimitiveDataListByType(TSDataType.INT64, true);
      timestamps.add(timeSpace);
      NVMSpaceMetadataManager.getInstance().registerTVSpace(timeSpace, valueSpace, sgId, deviceId, measurementId);
    }
  }

  @Override
  protected Object cloneTime(Object object) {
    NVMDataSpace timeSpace = (NVMDataSpace) object;
    return timeSpace.toArray();
  }

  public static NVMTVList newList(String sgId, String deviceId, String measurementId, TSDataType dataType) {
    switch (dataType) {
      case TEXT:
        return new NVMBinaryTVList(sgId, deviceId, measurementId);
      case FLOAT:
        return new NVMFloatTVList(sgId, deviceId, measurementId);
      case INT32:
        return new NVMIntTVList(sgId, deviceId, measurementId);
      case INT64:
        return new NVMLongTVList(sgId, deviceId, measurementId);
      case DOUBLE:
        return new NVMDoubleTVList(sgId, deviceId, measurementId);
      case BOOLEAN:
        return new NVMBooleanTVList(sgId, deviceId, measurementId);
    }
    return null;
  }

  public void loadData(List<NVMDataSpace> timeSpaceList, List<NVMDataSpace> valueSpaceList) {
    this.timestamps.addAll(timeSpaceList);
    this.values.addAll(valueSpaceList);

    refreshMetadata(timeSpaceList);
  }

  private void refreshMetadata(List<NVMDataSpace> spaceList) {
    if (spaceList.isEmpty()) {
      return;
    }

    NVMDataSpace lastSpace = spaceList.get(spaceList.size() - 1);
    int lastSpaceUnitSize = lastSpace.getValidUnitNum();

    // size
    for (int i = 0; i < spaceList.size() - 1; i++) {
      size += spaceList.get(i).getUnitNum();
    }
    size += lastSpaceUnitSize;

    // minTime
    for (int i = 0; i < spaceList.size() - 1; i++) {
      NVMDataSpace space = spaceList.get(i);
      for (int j = 0; j < space.getUnitNum(); j++) {
        minTime = Math.min(minTime, (Long) space.getData(j));
      }
    }
    for (int i = 0; i < lastSpaceUnitSize; i++) {
      minTime = Math.min(minTime, (Long) lastSpace.getData(i));
    }

    // sorted
    sorted = false;
  }

  @Override
  public void sort() {
    long time = System.currentTimeMillis();
    initTempArrays();
//    System.out.println("init arr:" + (System.currentTimeMillis() - time));
    PerfMonitor.add("sort-initarr", System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    copyTVToTempArrays();
//    System.out.println("copy to arr:" + (System.currentTimeMillis() - time));
    PerfMonitor.add("sort-copytoarr", System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    sort(0, size);
//    System.out.println("sort:" + (System.currentTimeMillis() - time));
    PerfMonitor.add("sort-sort", System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    copyTVFromTempArrays();
//    System.out.println("copy from arr:" + (System.currentTimeMillis() - time));
    PerfMonitor.add("sort-copyfromarr", System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    clearSortedValue();
    clearSortedTime();
//    System.out.println("clear arr:" + (System.currentTimeMillis() - time));
    PerfMonitor.add("sort-cleararr", System.currentTimeMillis() - time);

    sorted = true;
  }

  protected abstract void initTempArrays();

  protected abstract void copyTVToTempArrays();

  protected abstract void copyTVFromTempArrays();

  @Override
  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTimeForSort(lo);
      Object loV = getValueForSort(lo);
      long hiT = getTimeForSort(hi);
      Object hiV = getValueForSort(hi);
      setForSort(lo++, hiT, hiV);
      setForSort(hi--, loT, loV);
    }
  }

  @Override
  protected long getTimeForSort(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return tempTimestampsForSort[arrayIndex][elementIndex];
  }

  @Override
  protected void setForSort(int index, long timestamp, Object value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    tempTimestampsForSort[arrayIndex][elementIndex] = timestamp;
    setValueForSort(arrayIndex, elementIndex, value);
  }

  protected abstract void setValueForSort(int arrayIndex, int elementIndex, Object value);

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTimeForSort(pos);
    pivotValue = getValueForSort(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    setForSort(pos, pivotTime, pivotValue);
  }
}
