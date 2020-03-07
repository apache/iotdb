package org.apache.iotdb.tsfile.read.common;

import java.io.IOException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;

public class DataContainer {

  public static void main(String[] args) throws IOException {
//    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
//    long startTime = System.currentTimeMillis();
//    for (int i = 0; i < 99999999L; i++) {
//      batchData.putBoolean(i, true);
//    }
//    System.out.println("insert time :" + (System.currentTimeMillis() - startTime));
//
//    startTime = System.currentTimeMillis();
//    int count = 0;
//    while (batchData.hasCurrent()) {
//      batchData.currentTime();
//      batchData.currentValue();
//      batchData.next();
//      count++;
//    }
//    System.out
//        .println("read time :" + (System.currentTimeMillis() - startTime) + ", count:" + count);

    DataContainer batchData = new DataContainer(TSDataType.BOOLEAN);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 99999999L; i++) {
      batchData.put(i, true);
    }
    System.out.println("insert time :" + (System.currentTimeMillis() - startTime));

    startTime = System.currentTimeMillis();
    int count = 0;
    while (batchData.hasCurrent()) {
      batchData.currentTime();
      batchData.currentValue();
      batchData.next();
      count++;
    }
    System.out
        .println("read time :" + (System.currentTimeMillis() - startTime) + ", count:" + count);


//    DataContainer container = new DataContainer(TSDataType.BOOLEAN);
//    long[] data = new long[99999999];
//    boolean[] va = new boolean[99999999];
//    for (int i = 0; i < 99999999L; i++) {
//      data[i] = i;
//      va[i] = true;
//    }
//    long startTime = System.currentTimeMillis();
//    container.put(data, va);
//    System.out.println("insert time :" + (System.currentTimeMillis() - startTime));
//
//    startTime = System.currentTimeMillis();
//    int count = 0;
//    while (container.hasCurrent()) {
//      container.currentTime();
//      container.currentValue();
//      container.next();
//      count++;
//    }
//    System.out
//        .println("read time :" + (System.currentTimeMillis() - startTime) + ", count:" + count);

  }


  private TSDataType dataType;

  private Statistics statistics;

  private LongList timeColumn;

  private IoTDBArrayList valueColumn;

  private int currentReadIndex;

  public DataContainer(TSDataType dataType) {
    timeColumn = new LongList();
    statistics = Statistics.getStatsByType(dataType);
    switch (dataType) {
      case TEXT:
        break;
      case FLOAT:
      case INT32:
      case INT64:
        valueColumn = new LongList();
        break;
      case DOUBLE:
      case BOOLEAN:
        valueColumn = new BooleanList();
        break;
      default:
    }
  }

  public boolean hasCurrent() {
    return timeColumn.size > 0 && currentReadIndex < timeColumn.size;
  }

  public void next() {
    currentReadIndex++;
  }

  public long currentTime() {
    return timeColumn.getOriginValue(currentReadIndex);
  }

  public Object currentValue() {
    return valueColumn.getValue(currentReadIndex);
  }

  public void put(long time, long value) {
    statistics.update(time, value);
    timeColumn.put(time);
    valueColumn.put(value);
  }

  public void put(long time, int value) {
    statistics.update(time, value);
    timeColumn.put(time);
    valueColumn.put(value);
  }

  public void put(long time, float value) {
    statistics.update(time, value);
    timeColumn.put(time);
    valueColumn.put(value);
  }

  public void put(long time, double value) {
    statistics.update(time, value);
    timeColumn.put(time);
    valueColumn.put(value);
  }

  public void put(long time, Binary value) {
    statistics.update(time, value);
    timeColumn.put(time);
    valueColumn.put(value);
  }

  public void put(long time, boolean value) {
    statistics.update(time, value);
    timeColumn.put(time);
    valueColumn.put(value);
  }

  public void put(long[] time, long[] value) throws IOException {
    if (time.length != value.length) {
      throw new IOException("time and value must be align");
    }

    timeColumn.ensureCapacity(time.length);
    valueColumn.ensureCapacity(value.length);

    for (int i = 0; i < time.length; i++) {
      statistics.update(time[i], value[i]);
      timeColumn.fastPut(time[i]);
      valueColumn.fastPut(value[i]);
    }
  }

  public void put(long[] time, int[] value) throws IOException {
    if (time.length != value.length) {
      throw new IOException("time and value must be align");
    }

    timeColumn.ensureCapacity(time.length);
    valueColumn.ensureCapacity(value.length);

    for (int i = 0; i < time.length; i++) {
      statistics.update(time[i], value[i]);
      timeColumn.fastPut(time[i]);
      valueColumn.fastPut(value[i]);
    }
  }

  public void put(long[] time, float[] value) throws IOException {
    if (time.length != value.length) {
      throw new IOException("time and value must be align");
    }

    timeColumn.ensureCapacity(time.length);
    valueColumn.ensureCapacity(value.length);

    for (int i = 0; i < time.length; i++) {
      statistics.update(time[i], value[i]);
      timeColumn.fastPut(time[i]);
      valueColumn.fastPut(value[i]);
    }
  }

  public void put(long[] time, double[] value) throws IOException {
    if (time.length != value.length) {
      throw new IOException("time and value must be align");
    }

    timeColumn.ensureCapacity(time.length);
    valueColumn.ensureCapacity(value.length);

    for (int i = 0; i < time.length; i++) {
      statistics.update(time[i], value[i]);
      timeColumn.fastPut(time[i]);
      valueColumn.fastPut(value[i]);
    }
  }

  public void put(long[] time, Binary[] value) throws IOException {
    if (time.length != value.length) {
      throw new IOException("time and value must be align");
    }

    timeColumn.ensureCapacity(time.length);
    valueColumn.ensureCapacity(value.length);

    for (int i = 0; i < time.length; i++) {
      statistics.update(time[i], value[i]);
      timeColumn.fastPut(time[i]);
      valueColumn.fastPut(value[i]);
    }
  }

  public void put(long[] time, boolean[] value) throws IOException {
    if (time.length != value.length) {
      throw new IOException("time and value must be align");
    }

    timeColumn.ensureCapacity(time.length);
    valueColumn.ensureCapacity(value.length);

    for (int i = 0; i < time.length; i++) {
      statistics.update(time[i], value[i]);
      timeColumn.fastPut(time[i]);
      valueColumn.fastPut(value[i]);
    }
  }

}
