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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.LinkedList;

/**
 * This class is for reading and writing batch data in reverse. The data source is from mergeReader.
 * For example, the time sequence from mergeReader is 1000 -> 1, to keep the consistency that the
 * timestamp should be ascending. It will be written in reverse, i.e. the timeRet will be [1, 1000].
 * Then it can be handled the same as DescReadBatchData.
 */
public class DescReadWriteBatchData extends DescReadBatchData {

  public DescReadWriteBatchData(TSDataType dataType) {
    super();
    this.batchDataType = BatchDataType.DescReadWrite;
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
      case VECTOR:
        vectorRet = new LinkedList<>();
        vectorRet.add(new TsPrimitiveType[capacity][]);
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
  @Override
  public void putBoolean(long t, boolean v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<boolean[]>) booleanRet).addFirst(new boolean[capacity]);
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
  @Override
  public void putInt(long t, int v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<int[]>) intRet).addFirst(new int[capacity]);
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
  @Override
  public void putLong(long t, long v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<long[]>) longRet).addFirst(new long[capacity]);
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
  @Override
  public void putFloat(long t, float v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<float[]>) floatRet).addFirst(new float[capacity]);
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
  @Override
  public void putDouble(long t, double v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<double[]>) doubleRet).addFirst(new double[capacity]);
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
  @Override
  public void putBinary(long t, Binary v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<Binary[]>) binaryRet).addFirst(new Binary[capacity]);
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

  /**
   * put vector data.
   *
   * @param t timestamp
   * @param v vector data.
   */
  @Override
  public void putVector(long t, TsPrimitiveType[] v) {
    if (writeCurArrayIndex == -1) {
      if (capacity >= CAPACITY_THRESHOLD) {
        ((LinkedList<long[]>) timeRet).addFirst(new long[capacity]);
        ((LinkedList<TsPrimitiveType[][]>) vectorRet).addFirst(new TsPrimitiveType[capacity][]);
        writeCurListIndex++;
        writeCurArrayIndex = capacity - 1;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        TsPrimitiveType[][] newValueData = new TsPrimitiveType[newCapacity][];

        System.arraycopy(timeRet.get(0), 0, newTimeData, newCapacity - capacity, capacity);
        System.arraycopy(vectorRet.get(0), 0, newValueData, newCapacity - capacity, capacity);

        timeRet.set(0, newTimeData);
        vectorRet.set(0, newValueData);

        writeCurArrayIndex = newCapacity - capacity - 1;
        capacity = newCapacity;
      }
    }
    timeRet.get(0)[writeCurArrayIndex] = t;
    vectorRet.get(0)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  @Override
  public boolean hasCurrent() {
    return (readCurListIndex == 0 && readCurArrayIndex > writeCurArrayIndex)
        || (readCurListIndex > 0 && readCurArrayIndex >= 0);
  }

  @Override
  public void next() {
    super.readCurArrayIndex--;
    if ((readCurListIndex == 0 && readCurArrayIndex <= writeCurArrayIndex)
        || readCurArrayIndex == -1) {
      super.readCurListIndex--;
      super.readCurArrayIndex = capacity - 1;
    }
  }

  @Override
  public void resetBatchData() {
    super.readCurArrayIndex = capacity - 1;
    super.readCurListIndex = writeCurListIndex;
  }

  @Override
  public long getTimeByIndex(int idx) {
    return timeRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  @Override
  public long getLongByIndex(int idx) {
    return longRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  @Override
  public double getDoubleByIndex(int idx) {
    return doubleRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  @Override
  public int getIntByIndex(int idx) {
    return intRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  @Override
  public float getFloatByIndex(int idx) {
    return floatRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  @Override
  public Binary getBinaryByIndex(int idx) {
    return binaryRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  @Override
  public boolean getBooleanByIndex(int idx) {
    return booleanRet
        .get((idx + writeCurArrayIndex + 1) / capacity)[(idx + writeCurArrayIndex + 1) % capacity];
  }

  /**
   * Read: When put data, the writeIndex increases while the readIndex remains 0. For descending
   * read, we need to read from writeIndex to writeCurArrayIndex
   */
  @Override
  public BatchData flip() {
    super.readCurArrayIndex = capacity - 1;
    super.readCurListIndex = writeCurListIndex;
    return this;
  }
}
