package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.utils.Binary;

/**
 * @Author: LiuDaWei
 * @Create: 2020年03月06日
 */
public abstract class IoTDBArrayList {

  protected static final String ERR_DATATYPE_NOT_CONSISTENT = "DataType not consistent";

  protected int currentArrayIndex = 0;
  protected int currentInsideIndex = 0;
  protected int size = 0;

  public abstract void put(long value);

  public abstract void put(int value);

  public abstract void put(float value);

  public abstract void put(double value);

  public abstract void put(Binary value);

  public abstract void put(boolean value);

  public abstract void fastPut(long value);

  public abstract void fastPut(int value);

  public abstract void fastPut(float value);

  public abstract void fastPut(double value);

  public abstract void fastPut(Binary value);

  public abstract void fastPut(boolean value);

  public abstract Object getValue(int currentReadIndex);


  public void ensureCapacityInternal() {
    ensureCapacityInternal(size + 1);
  }


  protected void ensureCapacityInternal(int newSize) {
    if (currentArrayIndex << 10 > newSize) {
      return;
    }

    currentArrayIndex++;
    if (currentArrayIndex == getArrayLength()) {
      growArray();
    }
    initCurrentInsideArray();
    currentInsideIndex = 0;
    return;
  }

  protected abstract void initCurrentInsideArray();

  protected abstract int getArrayLength();

  protected abstract void growArray();
}
