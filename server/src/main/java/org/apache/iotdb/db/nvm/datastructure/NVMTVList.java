package org.apache.iotdb.db.nvm.datastructure;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSpace;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class NVMTVList extends AbstractTVList {

  protected List<NVMSpace> timestamps;
  protected List<NVMSpace> values;
  protected TSDataType dataType;

  public NVMTVList() {
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
    return (long) timestamps.get(arrayIndex).get(elementIndex);
  }

  protected void set(int index, long timestamp, Object value) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    timestamps.get(arrayIndex).set(elementIndex, timestamp);
    values.get(arrayIndex).set(elementIndex, value);
  }

  protected NVMSpace cloneValue(NVMSpace valueSpace) {
    return valueSpace.clone();
  }

  @Override
  protected void clearValue() {
    if (values != null) {
      for (NVMSpace valueSpace : values) {
        NVMPrimitiveArrayPool.getInstance().release(valueSpace, dataType);
      }
      values.clear();
    }
  }

  @Override
  protected void expandValues() {
    values.add(NVMPrimitiveArrayPool
        .getInstance().getPrimitiveDataListByType(dataType));
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
    NVMTVList cloneList = (NVMTVList) abstractCloneList;
    for (NVMSpace timeSpace : timestamps) {
      cloneList.timestamps.add((NVMSpace) cloneTime(timeSpace));
    }
    cloneList.size = size;
    cloneList.sorted = sorted;
    cloneList.minTime = minTime;
  }

  @Override
  protected void clearTime() {
    if (timestamps != null) {
      for (NVMSpace timeSpace : timestamps) {
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
  }

  @Override
  protected void checkExpansion() {
    if ((size % ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add(NVMPrimitiveArrayPool.getInstance().getPrimitiveDataListByType(TSDataType.INT64));
    }
  }

  @Override
  protected Object cloneTime(Object object) {
    NVMSpace timeSpace = (NVMSpace) object;
    return timeSpace.clone();
  }

  public static NVMTVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
        // TODO
//        return new BinaryTVList();
      case FLOAT:
        return new NVMFloatTVList();
      case INT32:
        return new NVMIntTVList();
      case INT64:
        return new NVMLongTVList();
      case DOUBLE:
        return new NVMDoubleTVList();
      case BOOLEAN:
        return new NVMBooleanTVList();
    }
    return null;
  }
}
