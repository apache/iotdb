/**
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

import java.util.ArrayList;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * <code>BatchData</code> is a self-defined data structure which is optimized for different type of
 * values. This class can be viewed as a collection which is more efficient than ArrayList.
 */
public class BatchData {

  private int timeCapacity = 1;
  private int valueCapacity = 1;
  private int emptyTimeCapacity = 1;
  private int capacityThreshold = 1024;

  private TSDataType dataType;
  private int curIdx;

  /** the number of ArrayList in timeRet **/
  private int timeArrayIdx;
  /** the index of current ArrayList in timeRet **/
  private int curTimeIdx;
  /** the insert timestamp number of timeRet **/
  private int timeLength;

  /** the number of ArrayList in valueRet **/
  private int valueArrayIdx;
  /** the index of current ArrayList in valueRet **/
  private int curValueIdx;
  /** the insert value number of valueRet **/
  private int valueLength;

  private ArrayList<long[]> timeRet;
  private ArrayList<long[]> emptyTimeRet;
  private ArrayList<boolean[]> booleanRet;
  private ArrayList<int[]> intRet;
  private ArrayList<long[]> longRet;
  private ArrayList<float[]> floatRet;
  private ArrayList<double[]> doubleRet;
  private ArrayList<Binary[]> binaryRet;

  public BatchData() {
    dataType = null;
  }

  public BatchData(TSDataType type) {
    dataType = type;
  }

  /**
   * BatchData Constructor.
   *
   * @param type Data type to record for this BatchData
   * @param recordTime whether to record time value for this BatchData
   */
  public BatchData(TSDataType type, boolean recordTime) {
    init(type, recordTime, false);
  }

  public BatchData(TSDataType type, boolean recordTime, boolean hasEmptyTime) {
    init(type, recordTime, hasEmptyTime);
  }

  public boolean hasNext() {
    return curIdx < timeLength;
  }

  public void next() {
    curIdx++;
  }

  public long currentTime() {
    rangeCheckForTime(curIdx);
    return this.timeRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
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

  public TSDataType getDataType() {
    return dataType;
  }

  /**
   * initialize batch data.
   *
   * @param type TSDataType
   * @param recordTime if record time
   * @param hasEmptyTime if has empty time
   */
  public void init(TSDataType type, boolean recordTime, boolean hasEmptyTime) {
    this.dataType = type;
    this.valueArrayIdx = 0;
    this.curValueIdx = 0;
    this.valueLength = 0;
    this.curIdx = 0;
    capacityThreshold = TSFileConfig.dynamicDataSize;

    if (recordTime) {
      timeRet = new ArrayList<>();
      timeRet.add(new long[timeCapacity]);
      timeArrayIdx = 0;
      curTimeIdx = 0;
      timeLength = 0;
    }

    if (hasEmptyTime) {
      emptyTimeRet = new ArrayList<>();
      emptyTimeRet.add(new long[emptyTimeCapacity]);
    }

    switch (dataType) {
      case BOOLEAN:
        booleanRet = new ArrayList<>();
        booleanRet.add(new boolean[valueCapacity]);
        break;
      case INT32:
        intRet = new ArrayList<>();
        intRet.add(new int[valueCapacity]);
        break;
      case INT64:
        longRet = new ArrayList<>();
        longRet.add(new long[valueCapacity]);
        break;
      case FLOAT:
        floatRet = new ArrayList<>();
        floatRet.add(new float[valueCapacity]);
        break;
      case DOUBLE:
        doubleRet = new ArrayList<>();
        doubleRet.add(new double[valueCapacity]);
        break;
      case TEXT:
        binaryRet = new ArrayList<>();
        binaryRet.add(new Binary[valueCapacity]);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  /**
   * put timestamp.
   *
   * @param v timestamp
   */
  public void putTime(long v) {
    if (curTimeIdx == timeCapacity) {
      if (timeCapacity >= capacityThreshold) {
        this.timeRet.add(new long[timeCapacity]);
        timeArrayIdx++;
        curTimeIdx = 0;
      } else {
        long[] newData = new long[timeCapacity * 2];
        System.arraycopy(timeRet.get(0), 0, newData, 0, timeCapacity);
        this.timeRet.set(0, newData);
        timeCapacity = timeCapacity * 2;
      }
    }
    (timeRet.get(timeArrayIdx))[curTimeIdx++] = v;
    timeLength++;
  }

  /**
   * put boolean data.
   *
   * @param v boolean data
   */
  public void putBoolean(boolean v) {
    if (curValueIdx == valueCapacity) {
      if (valueCapacity >= capacityThreshold) {
        if (this.booleanRet.size() <= valueArrayIdx + 1) {
          this.booleanRet.add(new boolean[valueCapacity]);
        }
        valueArrayIdx++;
        curValueIdx = 0;
      } else {
        boolean[] newData = new boolean[valueCapacity * 2];
        System.arraycopy(booleanRet.get(0), 0, newData, 0, valueCapacity);
        this.booleanRet.set(0, newData);
        valueCapacity = valueCapacity * 2;
      }
    }
    (this.booleanRet.get(valueArrayIdx))[curValueIdx++] = v;
    valueLength++;
  }

  /**
   * put int data.
   *
   * @param v int data
   */
  public void putInt(int v) {
    if (curValueIdx == valueCapacity) {
      if (valueCapacity >= capacityThreshold) {
        if (this.intRet.size() <= valueArrayIdx + 1) {
          this.intRet.add(new int[valueCapacity]);
        }
        valueArrayIdx++;
        curValueIdx = 0;
      } else {
        int[] newData = new int[valueCapacity * 2];
        System.arraycopy(intRet.get(0), 0, newData, 0, valueCapacity);
        this.intRet.set(0, newData);
        valueCapacity = valueCapacity * 2;
      }
    }
    (this.intRet.get(valueArrayIdx))[curValueIdx++] = v;
    valueLength++;
  }

  /**
   * put long data.
   *
   * @param v long data
   */
  public void putLong(long v) {
    if (curValueIdx == valueCapacity) {
      if (valueCapacity >= capacityThreshold) {
        if (this.longRet.size() <= valueArrayIdx + 1) {
          this.longRet.add(new long[valueCapacity]);
        }
        valueArrayIdx++;
        curValueIdx = 0;
      } else {
        long[] newData = new long[valueCapacity * 2];
        System.arraycopy(longRet.get(0), 0, newData, 0, valueCapacity);
        this.longRet.set(0, newData);
        valueCapacity = valueCapacity * 2;
      }
    }
    (this.longRet.get(valueArrayIdx))[curValueIdx++] = v;
    valueLength++;
  }

  /**
   * put float data.
   *
   * @param v float data
   */
  public void putFloat(float v) {
    if (curValueIdx == valueCapacity) {
      if (valueCapacity >= capacityThreshold) {
        if (this.floatRet.size() <= valueArrayIdx + 1) {
          this.floatRet.add(new float[valueCapacity]);
        }
        valueArrayIdx++;
        curValueIdx = 0;
      } else {
        float[] newData = new float[valueCapacity * 2];
        System.arraycopy(floatRet.get(0), 0, newData, 0, valueCapacity);
        this.floatRet.set(0, newData);
        valueCapacity = valueCapacity * 2;
      }
    }
    (this.floatRet.get(valueArrayIdx))[curValueIdx++] = v;
    valueLength++;
  }

  /**
   * put double data.
   *
   * @param v double data
   */
  public void putDouble(double v) {
    if (curValueIdx == valueCapacity) {
      if (valueCapacity >= capacityThreshold) {
        if (this.doubleRet.size() <= valueArrayIdx + 1) {
          this.doubleRet.add(new double[valueCapacity]);
        }
        valueArrayIdx++;
        curValueIdx = 0;
      } else {
        double[] newData = new double[valueCapacity * 2];
        System.arraycopy(doubleRet.get(0), 0, newData, 0, valueCapacity);
        this.doubleRet.set(0, newData);
        valueCapacity = valueCapacity * 2;
      }
    }
    (this.doubleRet.get(valueArrayIdx))[curValueIdx++] = v;
    valueLength++;
  }

  /**
   * put binary data.
   *
   * @param v binary data.
   */
  public void putBinary(Binary v) {
    if (curValueIdx == valueCapacity) {
      if (valueCapacity >= capacityThreshold) {
        if (this.binaryRet.size() <= valueArrayIdx + 1) {
          this.binaryRet.add(new Binary[valueCapacity]);
        }
        valueArrayIdx++;
        curValueIdx = 0;
      } else {
        Binary[] newData = new Binary[valueCapacity * 2];
        System.arraycopy(binaryRet.get(0), 0, newData, 0, valueCapacity);
        this.binaryRet.set(0, newData);
        valueCapacity = valueCapacity * 2;
      }
    }
    (this.binaryRet.get(valueArrayIdx))[curValueIdx++] = v;
    valueLength++;
  }

  /**
   * Checks if the given index is in range. If not, throws an appropriate runtime exception.
   */
  private void rangeCheck(int idx) {
    if (idx < 0) {
      throw new IndexOutOfBoundsException("BatchData value range check, Index is negative: " + idx);
    }
    if (idx >= valueLength) {
      throw new IndexOutOfBoundsException(
          "BatchData value range check, Index : " + idx + ". Length : " + valueLength);
    }
  }

  /**
   * Checks if the given index is in range. If not, throws an appropriate runtime exception.
   */
  private void rangeCheckForTime(int idx) {
    if (idx < 0) {
      throw new IndexOutOfBoundsException("BatchData time range check, Index is negative: " + idx);
    }
    if (idx >= timeLength) {
      throw new IndexOutOfBoundsException(
          "BatchData time range check, Index : " + idx + ". Length : " + timeLength);
    }
  }

  private void rangeCheckForEmptyTime(int idx) {
    if (idx < 0) {
      throw new IndexOutOfBoundsException(
          "BatchData empty time range check, Index is negative: " + idx);
    }
  }

  public boolean getBoolean() {
    rangeCheck(curIdx);
    return this.booleanRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
  }

  public void setBoolean(int idx, boolean v) {
    rangeCheck(idx);
    this.booleanRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public int getInt() {
    rangeCheck(curIdx);
    return this.intRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
  }

  public void setInt(int idx, int v) {
    rangeCheck(idx);
    this.intRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public long getLong() {
    rangeCheck(curIdx);
    return this.longRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
  }

  public void setLong(int idx, long v) {
    rangeCheck(idx);
    this.longRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public float getFloat() {
    rangeCheck(curIdx);
    return this.floatRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
  }

  public void setFloat(int idx, float v) {
    rangeCheck(idx);
    this.floatRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public double getDouble() {
    rangeCheck(curIdx);
    return this.doubleRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
  }

  public void setDouble(int idx, double v) {
    rangeCheck(idx);
    this.doubleRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public Binary getBinary() {
    rangeCheck(curIdx);
    return this.binaryRet.get(curIdx / timeCapacity)[curIdx % timeCapacity];
  }

  public void setBinary(int idx, Binary v) {
    this.binaryRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public void setTime(int idx, long v) {
    rangeCheckForTime(idx);
    this.timeRet.get(idx / timeCapacity)[idx % timeCapacity] = v;
  }

  public long getEmptyTime(int idx) {
    rangeCheckForEmptyTime(idx);
    return this.emptyTimeRet.get(idx / emptyTimeCapacity)[idx % emptyTimeCapacity];
  }

  /**
   * get time as array in long[] structure.
   *
   * @return time array
   */
  public long[] getTimeAsArray() {
    long[] res = new long[timeLength];
    for (int i = 0; i < timeLength; i++) {
      res[i] = timeRet.get(i / timeCapacity)[i % timeCapacity];
    }
    return res;
  }

  /**
   * put an object.
   *
   * @param v object
   */
  public void putAnObject(Object v) {
    switch (dataType) {
      case BOOLEAN:
        putBoolean((boolean) v);
        break;
      case INT32:
        putInt((int) v);
        break;
      case INT64:
        putLong((long) v);
        break;
      case FLOAT:
        putFloat((float) v);
        break;
      case DOUBLE:
        putDouble((double) v);
        break;
      case TEXT:
        putBinary((Binary) v);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  /**
   * set an object.
   *
   * @param idx object id
   * @param v object value
   */
  public void setAnObject(int idx, Comparable<?> v) {
    switch (dataType) {
      case BOOLEAN:
        setBoolean(idx, (Boolean) v);
        break;
      case DOUBLE:
        setDouble(idx, (Double) v);
        break;
      case TEXT:
        setBinary(idx, (Binary) v);
        break;
      case FLOAT:
        setFloat(idx, (Float) v);
        break;
      case INT32:
        setInt(idx, (Integer) v);
        break;
      case INT64:
        setLong(idx, (Long) v);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  public int length() {
    return this.timeLength;
  }
}
