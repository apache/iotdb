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
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsVector;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>BatchData</code> is a self-defined data structure which is optimized for different type of
 * values. This class can be viewed as a collection which is more efficient than ArrayList.
 *
 * <p>This class records a time list and a value list, which could be replaced by TVList in the
 * future
 *
 * <p>When you use BatchData in query process, it does not contain duplicated timestamps. The batch
 * data may be empty.
 *
 * <p>If you get a batch data, you can iterate the data as the following codes:
 *
 * <p>while (batchData.hasCurrent()) { long time = batchData.currentTime(); Object value =
 * batchData.currentValue(); batchData.next(); }
 */
public class BatchData {

  protected static final int CAPACITY_THRESHOLD = TSFileConfig.ARRAY_CAPACITY_THRESHOLD;
  protected int capacity = 16;

  protected TSDataType dataType;

  protected BatchDataType batchDataType = BatchDataType.Ordinary;

  // outer list index for read
  protected int readCurListIndex;
  // inner array index for read
  protected int readCurArrayIndex;

  // outer list index for write
  protected int writeCurListIndex;
  // inner array index for write
  protected int writeCurArrayIndex;

  // the insert timestamp number of timeRet
  protected int count;

  protected List<long[]> timeRet;
  protected List<boolean[]> booleanRet;
  protected List<int[]> intRet;
  protected List<long[]> longRet;
  protected List<float[]> floatRet;
  protected List<double[]> doubleRet;
  protected List<Binary[]> binaryRet;
  protected List<TsPrimitiveType[][]> vectorRet;

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
    if (readCurListIndex == writeCurListIndex) {
      return readCurArrayIndex < writeCurArrayIndex;
    }

    return readCurListIndex < writeCurListIndex && readCurArrayIndex < capacity;
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
      case VECTOR:
        return getVector();
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
      case VECTOR:
        return new TsVector(getVector());
      default:
        return null;
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public BatchDataType getBatchDataType() {
    return batchDataType;
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
      case VECTOR:
        vectorRet = new ArrayList<>();
        vectorRet.add(new TsPrimitiveType[capacity][]);
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
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        booleanRet.add(new boolean[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        boolean[] newValueData = new boolean[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(booleanRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        booleanRet.set(0, newValueData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    booleanRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        intRet.add(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        int[] newValueData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(intRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        intRet.set(0, newValueData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    intRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        longRet.add(new long[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        long[] newValueData = new long[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(longRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        longRet.set(0, newValueData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    longRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        floatRet.add(new float[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        float[] newValueData = new float[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(floatRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        floatRet.set(0, newValueData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    floatRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        doubleRet.add(new double[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        double[] newValueData = new double[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(doubleRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        doubleRet.set(0, newValueData);
        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    doubleRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        binaryRet.add(new Binary[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        Binary[] newValueData = new Binary[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(binaryRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        binaryRet.set(0, newValueData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    binaryRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

    writeCurArrayIndex++;
    count++;
  }

  /**
   * put vector data.
   *
   * @param t timestamp
   * @param v vector data.
   */
  public void putVector(long t, TsPrimitiveType[] v) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= CAPACITY_THRESHOLD) {
        timeRet.add(new long[capacity]);
        vectorRet.add(new TsPrimitiveType[capacity][]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        TsPrimitiveType[][] newValueData = new TsPrimitiveType[newCapacity][];

        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        System.arraycopy(vectorRet.get(0), 0, newValueData, 0, capacity);

        timeRet.set(0, newTimeData);
        vectorRet.set(0, newValueData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    vectorRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

    writeCurArrayIndex++;
    count++;
  }

  public boolean getBoolean() {
    return this.booleanRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setBoolean(boolean v) {
    this.booleanRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public int getInt() {
    return this.intRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setInt(int v) {
    this.intRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public long getLong() {
    return this.longRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setLong(long v) {
    this.longRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public float getFloat() {
    return this.floatRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setFloat(float v) {
    this.floatRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public double getDouble() {
    return this.doubleRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setDouble(double v) {
    this.doubleRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public Binary getBinary() {
    return this.binaryRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setBinary(Binary v) {
    this.binaryRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public TsPrimitiveType[] getVector() {
    return this.vectorRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void setVector(TsPrimitiveType[] v) {
    this.vectorRet.get(readCurListIndex)[readCurArrayIndex] = v;
  }

  public void setTime(long v) {
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
      case VECTOR:
        putVector(t, (TsPrimitiveType[]) v);
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

  public TsPrimitiveType[] getVectorByIndex(int idx) {
    return vectorRet.get(idx / capacity)[idx % capacity];
  }

  public TimeValuePair getLastPairBeforeOrEqualTimestamp(long queryTime) {
    TimeValuePair resultPair = new TimeValuePair(Long.MIN_VALUE, null);
    resetBatchData();
    while (hasCurrent() && (currentTime() <= queryTime)) {
      resultPair.setTimestamp(currentTime());
      resultPair.setValue(currentTsPrimitiveType());
      next();
    }
    return resultPair;
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

  public long getMaxTimestamp() {
    return getTimeByIndex(length() - 1);
  }

  public long getMinTimestamp() {
    return getTimeByIndex(0);
  }

  public BatchDataIterator getBatchDataIterator() {
    return new BatchDataIterator();
  }

  /** Only used for the batch data of vector time series. */
  public IBatchDataIterator getBatchDataIterator(int subIndex) {
    return new VectorBatchDataIterator(subIndex);
  }

  /**
   * This method is used to reset batch data when more than one group by aggregation functions visit
   * the same batch data
   */
  public void resetBatchData() {
    this.readCurArrayIndex = 0;
    this.readCurListIndex = 0;
  }

  public void resetBatchData(int readCurArrayIndex, int readCurListIndex) {
    this.readCurArrayIndex = readCurArrayIndex;
    this.readCurListIndex = readCurListIndex;
  }

  public int getReadCurListIndex() {
    return readCurListIndex;
  }

  public int getReadCurArrayIndex() {
    return readCurArrayIndex;
  }

  /**
   * When put data, the writeIndex increases while the readIndex remains 0. For ascending read, we
   * could read from 0 to writeIndex. So no need to flip.
   */
  public BatchData flip() {
    return this;
  }

  public enum BatchDataType {
    Ordinary,
    DescRead,
    DescReadWrite;

    BatchDataType() {}

    /**
     * give an integer to return a BatchType type.
     *
     * @param type -param to judge enum type
     * @return -enum type
     */
    public static BatchData deserialize(byte type, TSDataType dataType) {
      switch (type) {
        case 0:
          return new BatchData(dataType);
        case 1:
          return new DescReadBatchData(dataType);
        case 2:
          return new DescReadWriteBatchData(dataType);
        default:
          throw new IllegalArgumentException("Invalid input: " + type);
      }
    }
  }

  private class BatchDataIterator implements IPointReader, IBatchDataIterator {

    @Override
    public boolean hasNext() {
      return BatchData.this.hasCurrent();
    }

    @Override
    public void next() {
      BatchData.this.next();
    }

    @Override
    public long currentTime() {
      return BatchData.this.currentTime();
    }

    @Override
    public Object currentValue() {
      return BatchData.this.currentValue();
    }

    @Override
    public void reset() {
      BatchData.this.resetBatchData();
    }

    @Override
    public int totalLength() {
      return BatchData.this.length();
    }

    @Override
    public boolean hasNextTimeValuePair() {
      return hasNext();
    }

    @Override
    public TimeValuePair nextTimeValuePair() {
      TimeValuePair timeValuePair = new TimeValuePair(currentTime(), currentTsPrimitiveType());
      next();
      return timeValuePair;
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return new TimeValuePair(currentTime(), currentTsPrimitiveType());
    }

    @Override
    public void close() {}
  }

  private class VectorBatchDataIterator extends BatchDataIterator {

    private final int subIndex;

    private VectorBatchDataIterator(int subIndex) {
      this.subIndex = subIndex;
    }

    @Override
    public Object currentValue() {
      if (dataType == TSDataType.VECTOR) {
        return getVector()[subIndex].getValue();
      } else {
        return null;
      }
    }
  }
}
