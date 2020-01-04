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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.*;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * <code>BatchData</code> is a self-defined data structure which is optimized for different type of
 * values. This class can be viewed as a collection which is more efficient than ArrayList.
 *
 * This class records a time list and a value list, which could be replaced by TVList in the future
 *
 * When you use BatchData in query process, it does not contain duplicated timestamps. The batch data
 * may be empty.
 *
 * If you get a batch data, you can iterate the data as the following codes:
 *
 * while (batchData.hasCurrent()) {
 *   long time = batchData.currentTime();
 *   Object value = batchData.currentValue();
 *   batchData.next();
 * }
 */
public class BatchData implements Serializable {

  private static final long serialVersionUID = -4620310601188394839L;
  private int capacity = 16;
  private int capacityThreshold = 1024;

  private TSDataType dataType;

  // outer list index for read
  private int readCurListIndex;
  // inner array index for read
  private int readCurArrayIndex;

  // outer list index for write
  private int writeCurListIndex;
  // inner array index for write
  private int writeCurArrayIndex;

  // the insert timestamp number of timeRet
  private int count;


  private ArrayList<long[]> timeRet;
  private ArrayList<boolean[]> booleanRet;
  private ArrayList<int[]> intRet;
  private ArrayList<long[]> longRet;
  private ArrayList<float[]> floatRet;
  private ArrayList<double[]> doubleRet;
  private ArrayList<Binary[]> binaryRet;

  public BatchData() {
    dataType = null;
  }

  /**
   * BatchData Constructor.
   *
   * @param type Data type to record for this BatchData
   */
  public BatchData(TSDataType type) {
    init(type);
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public boolean hasCurrent() {
    if (readCurListIndex < writeCurListIndex) {
      return readCurArrayIndex < capacity;
    }
    else if (readCurListIndex == writeCurListIndex) {
      return readCurArrayIndex < writeCurArrayIndex;
    }
    else {
      return false;
    }
  }

  public void next() {
    readCurArrayIndex++;
    if (readCurArrayIndex == capacity) {
      readCurArrayIndex = 0;
      readCurListIndex++;
    }
  }

  public long currentTime() {
    return this.timeRet.get(readCurListIndex)[readCurArrayIndex];
  }

  /**
   * get current value.
   *
   * @return current value
   */
  public Object currentValue() {
    switch (dataType) {
      case INT32:
        return getInt();
      case INT64:
        return getLong();
      case FLOAT:
        return getFloat();
      case DOUBLE:
        return getDouble();
      case BOOLEAN:
        return getBoolean();
      case TEXT:
        return getBinary();
      default:
        return null;
    }
  }

  public TsPrimitiveType currentTsPrimitiveType() {
    switch (dataType) {
      case INT32:
        return new TsInt(getInt());
      case INT64:
        return new TsLong(getLong());
      case FLOAT:
        return new TsFloat(getFloat());
      case DOUBLE:
        return new TsDouble(getDouble());
      case BOOLEAN:
        return new TsBoolean(getBoolean());
      case TEXT:
        return new TsBinary(getBinary());
      default:
        return null;
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  /**
   * initialize batch data.
   *
   * @param type TSDataType
   */
  public void init(TSDataType type) {
    this.dataType = type;
    this.readCurListIndex = 0;
    this.readCurArrayIndex = 0;
    this.writeCurListIndex = 0;
    this.writeCurArrayIndex = 0;
    capacityThreshold = TSFileConfig.DYNAMIC_DATA_SIZE;

    timeRet = new ArrayList<>();
    timeRet.add(new long[capacity]);
    count = 0;

    switch (dataType) {
      case BOOLEAN:
        booleanRet = new ArrayList<>();
        booleanRet.add(new boolean[capacity]);
        break;
      case INT32:
        intRet = new ArrayList<>();
        intRet.add(new int[capacity]);
        break;
      case INT64:
        longRet = new ArrayList<>();
        longRet.add(new long[capacity]);
        break;
      case FLOAT:
        floatRet = new ArrayList<>();
        floatRet.add(new float[capacity]);
        break;
      case DOUBLE:
        doubleRet = new ArrayList<>();
        doubleRet.add(new double[capacity]);
        break;
      case TEXT:
        binaryRet = new ArrayList<>();
        binaryRet.add(new Binary[capacity]);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  /**
   * put boolean data.
   *
   * @param t timestamp
   * @param v boolean data
   */
  public void putBoolean(long t, boolean v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        booleanRet.add(new boolean[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        long[] newTimeData = new long[capacity * 2];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);
        boolean[] newValueData = new boolean[capacity * 2];
        System.arraycopy(booleanRet.get(0), 0, newValueData, 0, capacity);
        booleanRet.set(0, newValueData);
        capacity = capacity * 2;
      }
    }
    (timeRet.get(writeCurListIndex))[writeCurArrayIndex] = t;
    (booleanRet.get(writeCurListIndex))[writeCurArrayIndex] = v;
    writeCurArrayIndex++;
    count++;
  }

  /**
   * put int data.
   *
   * @param t timestamp
   * @param v int data
   */
  public void putInt(long t, int v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        intRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        long[] newTimeData = new long[capacity * 2];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);
        int[] newValueData = new int[capacity * 2];
        System.arraycopy(intRet.get(0), 0, newValueData, 0, capacity);
        intRet.set(0, newValueData);
        capacity = capacity * 2;
      }
    }
    (timeRet.get(writeCurListIndex))[writeCurArrayIndex] = t;
    (intRet.get(writeCurListIndex))[writeCurArrayIndex] = v;
    writeCurArrayIndex++;
    count++;
  }

  /**
   * put long data.
   *
   * @param t timestamp
   * @param v long data
   */
  public void putLong(long t, long v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        longRet.add(new long[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        long[] newTimeData = new long[capacity * 2];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);
        long[] newValueData = new long[capacity * 2];
        System.arraycopy(longRet.get(0), 0, newValueData, 0, capacity);
        longRet.set(0, newValueData);
        capacity = capacity * 2;
      }
    }
    (timeRet.get(writeCurListIndex))[writeCurArrayIndex] = t;
    (longRet.get(writeCurListIndex))[writeCurArrayIndex] = v;
    writeCurArrayIndex++;
    count++;
  }

  /**
   * put float data.
   *
   * @param t timestamp
   * @param v float data
   */
  public void putFloat(long t, float v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        floatRet.add(new float[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        long[] newTimeData = new long[capacity * 2];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);
        float[] newValueData = new float[capacity * 2];
        System.arraycopy(floatRet.get(0), 0, newValueData, 0, capacity);
        floatRet.set(0, newValueData);
        capacity = capacity * 2;
      }
    }
    (timeRet.get(writeCurListIndex))[writeCurArrayIndex] = t;
    (floatRet.get(writeCurListIndex))[writeCurArrayIndex] = v;
    writeCurArrayIndex++;
    count++;
  }

  /**
   * put double data.
   *
   * @param t timestamp
   * @param v double data
   */
  public void putDouble(long t, double v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        doubleRet.add(new double[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        long[] newTimeData = new long[capacity * 2];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);
        double[] newValueData = new double[capacity * 2];
        System.arraycopy(doubleRet.get(0), 0, newValueData, 0, capacity);
        doubleRet.set(0, newValueData);
        capacity = capacity * 2;
      }
    }
    (timeRet.get(writeCurListIndex))[writeCurArrayIndex] = t;
    (doubleRet.get(writeCurListIndex))[writeCurArrayIndex] = v;
    writeCurArrayIndex++;
    count++;
  }

  /**
   * put binary data.
   *
   * @param t timestamp
   * @param v binary data.
   */
  public void putBinary(long t, Binary v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        binaryRet.add(new Binary[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        long[] newTimeData = new long[capacity * 2];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);
        Binary[] newValueData = new Binary[capacity * 2];
        System.arraycopy(binaryRet.get(0), 0, newValueData, 0, capacity);
        binaryRet.set(0, newValueData);
        capacity = capacity * 2;
      }
    }
    (timeRet.get(writeCurListIndex))[writeCurArrayIndex] = t;
    (binaryRet.get(writeCurListIndex))[writeCurArrayIndex] = v;
    writeCurArrayIndex++;
    count++;
  }



  public boolean getBoolean() {
    return this.booleanRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setBoolean(int idx, boolean v) {
    this.booleanRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public int getInt() {
    return this.intRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setInt(int idx, int v) {
    this.intRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public long getLong() {
    return this.longRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setLong(int idx, long v) {
    this.longRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public float getFloat() {
    return this.floatRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setFloat(int idx, float v) {
    this.floatRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public double getDouble() {
    return this.doubleRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setDouble(int idx, double v) {
    this.doubleRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public Binary getBinary() {
    return this.binaryRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setBinary(int idx, Binary v) {
    this.binaryRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public void setTime(int idx, long v) {
    this.timeRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  /**
   * put an object.
   *
   * @param t timestamp
   * @param v object
   */
  public void putAnObject(long t, Object v) {
    switch (dataType) {
      case BOOLEAN:
        putBoolean(t, (boolean) v);
        break;
      case INT32:
        putInt(t, (int) v);
        break;
      case INT64:
        putLong(t, (long) v);
        break;
      case FLOAT:
        putFloat(t, (float) v);
        break;
      case DOUBLE:
        putDouble(t, (double) v);
        break;
      case TEXT:
        putBinary(t, (Binary) v);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  public int length() {
    return this.count;
  }

  public long getTimeByIndex(int idx) {
    return this.timeRet.get(idx / capacity)[idx % capacity];
  }

  public long getLongByIndex(int idx) {
    return this.longRet.get(idx / capacity)[idx % capacity];
  }

  public double getDoubleByIndex(int idx) {
    return this.doubleRet.get(idx / capacity)[idx % capacity];
  }

  public int getIntByIndex(int idx) {
    return this.intRet.get(idx / capacity)[idx % capacity];
  }

  public float getFloatByIndex(int idx) {
    return this.floatRet.get(idx / capacity)[idx % capacity];
  }

  public Binary getBinaryByIndex(int idx) {
    return binaryRet.get(idx / capacity)[idx % capacity];
  }

  public boolean getBooleanByIndex(int idx) {
    return booleanRet.get(idx / capacity)[idx % capacity];
  }

  public Object getValueInTimestamp(long time) {
    while (hasCurrent()) {
      if (currentTime() < time) {
        next();
      } else if (currentTime() == time) {
        Object value = currentValue();
        next();
        return value;
      } else {
        return null;
      }
    }
    return null;
  }
}
