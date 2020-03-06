package org.apache.iotdb.tsfile.read.common;

import java.util.Arrays;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * @Author: LiuDaWei
 * @Create: 2020年03月06日
 */
public class LongList extends IoTDBArrayList {

  private long[][] elementData = new long[1][1024];

  @Override
  public void put(long value) {
    ensureCapacityInternal();
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }

  @Override
  public void fastPut(long value) {
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }


  @Override
  public Object getValue(int currentReadIndex) {
    return elementData[currentReadIndex / 1024][currentReadIndex % 1024];
  }

  @Override
  protected void initCurrentInsideArray() {
    if (elementData[currentArrayIndex] == null) {
      elementData[currentArrayIndex] = new long[1024];
    }
  }

  @Override
  protected int getArrayLength() {
    return elementData.length;
  }

  @Override
  protected void growArray() {
    elementData = Arrays.copyOf(elementData, elementData.length * 2);
  }


  @Override
  public void put(int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void put(float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void put(double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void put(Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void put(boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void fastPut(int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void fastPut(float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void fastPut(double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void fastPut(Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @Override
  public void fastPut(boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getOriginValue(int index) {
    return elementData[index / 1024][index % 1024];
  }
}
