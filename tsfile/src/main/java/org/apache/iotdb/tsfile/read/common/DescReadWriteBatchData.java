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

import java.util.LinkedList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * This class is for reading and writing batch data in reverse. The data source is from mergeReader.
 * For example,
 * the time sequence from mergeReader is 1000 -> 1,
 * It will be written in reverse, i.e. the timeRet will be [1, 1000].
 * and the sequence of reading will be from back to front, 1000 -> 1.
 */
public class DescReadWriteBatchData extends DescReadBatchData {

  public DescReadWriteBatchData(TSDataType dataType) {
    super();
    this.dataType = dataType;
    this.readCurListIndex = 0;
    this.readCurArrayIndex = 0;
    this.writeCurListIndex = 0;
    this.writeCurArrayIndex = capacity - 1;

    timeRet = new LinkedList<>();
    timeRet.add(new long[capacity]);
    count = 0;

    switch (dataType) {
      case BOOLEAN:
        booleanRet = new LinkedList<>();
        booleanRet.add(new boolean[capacity]);
        break;
      case INT32:
        intRet = new LinkedList<>();
        intRet.add(new int[capacity]);
        break;
      case INT64:
        longRet = new LinkedList<>();
        longRet.add(new long[capacity]);
        break;
      case FLOAT:
        floatRet = new LinkedList<>();
        floatRet.add(new float[capacity]);
        break;
      case DOUBLE:
        doubleRet = new LinkedList<>();
        doubleRet.add(new double[capacity]);
        break;
      case TEXT:
        binaryRet = new LinkedList<>();
        binaryRet.add(new Binary[capacity]);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  /**
   * put boolean data reversely.
   *
   * @param t timestamp
   * @param v boolean data
   */
  public void putBoolean(long t, boolean v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= capacityThreshold) {
        ((LinkedList) timeRet).addFirst(new long[capacity]);
        ((LinkedList) booleanRet).addFirst(new boolean[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        boolean[] newValueData = new boolean[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(booleanRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        booleanRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    booleanRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  /**
   * put int data reversely.
   *
   * @param t timestamp
   * @param v int data
   */
  public void putInt(long t, int v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= capacityThreshold) {
        ((LinkedList) timeRet).addFirst(new long[capacity]);
        ((LinkedList) intRet).addFirst(new int[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        int[] newValueData = new int[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(intRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        intRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    intRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  /**
   * put long data reversely.
   *
   * @param t timestamp
   * @param v long data
   */
  public void putLong(long t, long v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= capacityThreshold) {
        ((LinkedList) timeRet).addFirst(new long[capacity]);
        ((LinkedList) longRet).addFirst(new long[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        long[] newValueData = new long[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(longRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        longRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    longRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  /**
   * put float data reversely.
   *
   * @param t timestamp
   * @param v float data
   */
  public void putFloat(long t, float v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= capacityThreshold) {
        ((LinkedList) timeRet).addFirst(new long[capacity]);
        ((LinkedList) floatRet).addFirst(new float[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        float[] newValueData = new float[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(floatRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        floatRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    floatRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  /**
   * put double data reversely.
   *
   * @param t timestamp
   * @param v double data
   */
  public void putDouble(long t, double v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= capacityThreshold) {
        ((LinkedList) timeRet).addFirst(new long[capacity]);
        ((LinkedList) doubleRet).addFirst(new double[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        double[] newValueData = new double[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(doubleRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        doubleRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    doubleRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  /**
   * put binary data reversely.
   *
   * @param t timestamp
   * @param v binary data.
   */
  public void putBinary(long t, Binary v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= capacityThreshold) {
        ((LinkedList) timeRet).addFirst(new long[capacity]);
        ((LinkedList) binaryRet).addFirst(new Binary[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        Binary[] newValueData = new Binary[newCapacity];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(binaryRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        binaryRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    binaryRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  @Override
  public void next() {
    super.readCurArrayIndex--;
    if (super.readCurArrayIndex == -1) {
      super.readCurListIndex--;
      if (readCurListIndex == 0) {
        super.readCurArrayIndex = writeCurArrayIndex - 1;
      } else {
        super.readCurArrayIndex = capacity - 1;
      }
    }
  }

  @Override
  public void resetBatchData() {
    if (writeCurListIndex >= 1) {
      super.readCurArrayIndex = capacity - 1;
    } else {
      super.readCurArrayIndex = writeCurArrayIndex - 1;
    }
    super.readCurListIndex = writeCurListIndex;
  }

  public long getTimeByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.timeRet.get(0)[idx];
    } else {
      return this.timeRet.get((idx - writeCurArrayIndex) / capacity + 1)[(idx - writeCurArrayIndex)
          % capacity];
    }
  }

  public long getLongByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.longRet.get(0)[idx];
    } else {
      return this.longRet.get((idx - writeCurArrayIndex) / capacity + 1)[(idx - writeCurArrayIndex)
          % capacity];
    }
  }

  public double getDoubleByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.doubleRet.get(0)[idx];
    } else {
      return this.doubleRet.get((idx - writeCurArrayIndex) / capacity + 1)[
          (idx - writeCurArrayIndex) % capacity];
    }
  }

  public int getIntByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.intRet.get(0)[idx];
    } else {
      return this.intRet.get((idx - writeCurArrayIndex) / capacity + 1)[(idx - writeCurArrayIndex)
          % capacity];
    }
  }

  public float getFloatByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.floatRet.get(0)[idx];
    } else {
      return this.floatRet.get((idx - writeCurArrayIndex) / capacity + 1)[(idx - writeCurArrayIndex)
          % capacity];
    }
  }

  public Binary getBinaryByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.binaryRet.get(0)[idx];
    } else {
      return this.binaryRet.get((idx - writeCurArrayIndex) / capacity + 1)[
          (idx - writeCurArrayIndex) % capacity];
    }
  }

  public boolean getBooleanByIndex(int idx) {
    if (idx < writeCurArrayIndex) {
      return this.booleanRet.get(0)[idx];
    } else {
      return this.booleanRet.get((idx - writeCurArrayIndex) / capacity + 1)[
          (idx - writeCurArrayIndex) % capacity];
    }
  }

  /**
   * Read: When put data, the writeIndex increases while the readIndex remains 0.
   * For descending read, we need to read from writeIndex to 0 (set the readIndex to writeIndex)
   */
  @Override
  public BatchData flip() {
    // if the end Index written > 0, we copy the elements to the front
    // e.g. when capacity = 32, writeCurArrayIndex = 14
    // copy elements [15, 31] -> [0, 16]
    int length = capacity - writeCurArrayIndex - 1;
    if (writeCurArrayIndex > 0) {
      System.arraycopy(timeRet.get(0), writeCurArrayIndex + 1, timeRet.get(0), 0, length);
      switch (dataType) {
        case BOOLEAN:
          System
              .arraycopy(booleanRet.get(0), writeCurArrayIndex + 1, booleanRet.get(0), 0, length);
          break;
        case INT32:
          System.arraycopy(intRet.get(0), writeCurArrayIndex + 1, intRet.get(0), 0, length);
          break;
        case INT64:
          System.arraycopy(longRet.get(0), writeCurArrayIndex + 1, longRet.get(0), 0, length);
          break;
        case FLOAT:
          System.arraycopy(floatRet.get(0), writeCurArrayIndex + 1, floatRet.get(0), 0, length);
          break;
        case DOUBLE:
          System.arraycopy(doubleRet.get(0), writeCurArrayIndex + 1, doubleRet.get(0), 0, length);
          break;
        case TEXT:
          System.arraycopy(binaryRet.get(0), writeCurArrayIndex + 1, binaryRet.get(0), 0, length);
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    writeCurArrayIndex = length;

    super.readCurArrayIndex = writeCurListIndex > 0 ? capacity - 1 : writeCurArrayIndex - 1;
    super.readCurListIndex = writeCurListIndex;
    return this;
  }

}
