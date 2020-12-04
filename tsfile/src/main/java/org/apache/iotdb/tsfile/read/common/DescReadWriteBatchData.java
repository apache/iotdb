package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * This class is for reading and writing batch data reversely. The data source is from mergeReader.
 * For example,
 * the time sequence from mergeReader is [1000, 1],
 * It will be written in descending sequence, i.e. [1, 1000],
 * and the sequence of reading will be 1000 -> 1.
 */
public class DescReadWriteBatchData extends DescReadBatchData {

  private int firstListSize;

  public DescReadWriteBatchData(TSDataType dataType) {
    super(dataType);
    this.writeCurArrayIndex = capacity - 1;
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
        timeRet.add(new long[capacity]);
        booleanRet.add(new boolean[capacity]);
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
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    booleanRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
        timeRet.add(new long[capacity]);
        intRet.add(new int[capacity]);
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
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    intRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
        timeRet.add(new long[capacity]);
        longRet.add(new long[capacity]);
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
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    longRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
        timeRet.add(new long[capacity]);
        floatRet.add(new float[capacity]);
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
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    floatRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
        timeRet.add(new long[capacity]);
        doubleRet.add(new double[capacity]);
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
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    doubleRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

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
        timeRet.add(new long[capacity]);
        binaryRet.add(new Binary[capacity]);
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
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = t;
    binaryRet.get(writeCurListIndex)[writeCurArrayIndex] = v;

    writeCurArrayIndex--;
    count++;
  }

  @Override
  public void next() {
    super.readCurArrayIndex--;
    if (super.readCurArrayIndex == -1) {
      super.readCurListIndex--;
      if (readCurListIndex == 0) {
        super.readCurArrayIndex = firstListSize - 1;
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
      super.readCurArrayIndex = firstListSize - 1;
    }
    super.readCurListIndex = writeCurListIndex;
  }


  /**
   * Write: put the
   * Read: When put data, the writeIndex increases while the readIndex remains 0.
   * For descending read, we need to read from writeIndex to 0 (set the readIndex to writeIndex)
   */
  @Override
  public BatchData flip() {

    // if the end Index of written > 0, we copy the elements to the first
    // e.g. when capacity = 32, writeCurArrayIndex = 14
    // copy elements [15, 31] -> [0, 16]
    int length = capacity - writeCurArrayIndex - 1;
    if (writeCurArrayIndex > 0) {
      System.arraycopy(timeRet.get(writeCurListIndex), writeCurArrayIndex + 1,
          timeRet.get(writeCurListIndex), 0, length);
      valueRetCopy(writeCurListIndex, writeCurArrayIndex + 1, writeCurListIndex, 0, length);
    }
    firstListSize = length;

    // if multi lists are written, exchange the order
    // e.g. current lists are [3001, 4000], [2001, 3000], [1001, 2000], [1, 600]
    // exchange the order to [1, 600], [1001, 2000], [2001, 3000], [3001, 4000]
    if (writeCurListIndex >= 1) {
      timeRet.add(new long[capacity]);
      addValue(capacity);

      for (int i = 0; i < (writeCurListIndex + 1) / 2; i++) {
        // writeCurListIndex + 1 is temp variable
        System.arraycopy(timeRet.get(i), 0, timeRet.get(writeCurListIndex + 1), 0 ,capacity);
        valueRetCopy(i, 0, writeCurListIndex + 1, 0, capacity);

        System.arraycopy(timeRet.get(writeCurListIndex - i), 0, timeRet.get(i), 0, capacity);
        valueRetCopy(writeCurListIndex - i, 0, i, 0, capacity);
        System.arraycopy(timeRet.get(writeCurListIndex + 1), 0, timeRet.get(writeCurListIndex - i), 0, capacity);
        valueRetCopy(writeCurListIndex + 1, 0,writeCurListIndex - i, 0, capacity);
      }

      timeRet.remove(writeCurListIndex + 1);
      removeValue(writeCurListIndex + 1);
    }

    super.readCurArrayIndex = writeCurListIndex > 0 ? capacity - 1 : firstListSize - 1;
    super.readCurListIndex = writeCurListIndex;
    return this;
  }

  private void valueRetCopy(int srcIndex, int srcPos, int desIndex, int descPos, int length) {
    switch (dataType) {
      case BOOLEAN:
        System
            .arraycopy(booleanRet.get(srcIndex), srcPos, booleanRet.get(desIndex), descPos, length);
        break;
      case INT32:
        System.arraycopy(intRet.get(srcIndex), srcPos, intRet.get(desIndex), descPos, length);
        break;
      case INT64:
        System.arraycopy(longRet.get(srcIndex), srcPos, longRet.get(desIndex), descPos, length);
        break;
      case FLOAT:
        System.arraycopy(floatRet.get(srcIndex), srcPos, floatRet.get(desIndex), descPos, length);
        break;
      case DOUBLE:
        System.arraycopy(doubleRet.get(srcIndex), srcPos, doubleRet.get(desIndex), descPos, length);
        break;
      case TEXT:
        System.arraycopy(binaryRet.get(srcIndex), srcPos, binaryRet.get(desIndex), descPos, length);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  private void addValue(int length) {
    switch (dataType) {
      case BOOLEAN:
        booleanRet.add(new boolean[length]);
        break;
      case INT32:
        intRet.add(new int[length]);
        break;
      case INT64:
        longRet.add(new long[length]);
        break;
      case FLOAT:
        floatRet.add(new float[length]);
        break;
      case DOUBLE:
        doubleRet.add(new double[length]);
        break;
      case TEXT:
        binaryRet.add(new Binary[length]);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  private void removeValue(int index) {
    switch (dataType) {
      case BOOLEAN:
        booleanRet.remove(index);
        break;
      case INT32:
        intRet.remove(index);
        break;
      case INT64:
        longRet.remove(index);
        break;
      case FLOAT:
        floatRet.remove(index);
        break;
      case DOUBLE:
        doubleRet.remove(index);
        break;
      case TEXT:
        binaryRet.remove(index);
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }
}
