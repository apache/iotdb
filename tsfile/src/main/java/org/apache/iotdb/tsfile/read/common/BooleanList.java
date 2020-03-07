package org.apache.iotdb.tsfile.read.common;

import java.util.Arrays;

/**
 * @Author: LiuDaWei
 * @Create: 2020年03月06日
 */
public class BooleanList extends IoTDBArrayList {

  private boolean[][] elementData = new boolean[ARRAY_INIT_SIZE][];

  public BooleanList() {
    initInsideArray(0);
  }

  @Override
  public void put(boolean value) {
    ensureCapacityInternal();
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }

  @Override
  public void fastPut(boolean value) {
    if (currentInsideIndex == INSIDE_ARRAY_INIT_SIZE) {
      currentArrayIndex++;
      currentInsideIndex = 0;
    }
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }


  @Override
  public Object getValue(int currentReadIndex) {
    return elementData[currentReadIndex / INSIDE_ARRAY_INIT_SIZE]
        [currentReadIndex & (INSIDE_ARRAY_INIT_SIZE - 1)];
  }


  public boolean getOriginValue(int currentReadIndex) {
    return elementData[currentReadIndex / INSIDE_ARRAY_INIT_SIZE]
        [currentReadIndex & (INSIDE_ARRAY_INIT_SIZE - 1)];
  }

  @Override
  protected void initInsideArray(int index) {
    if (elementData[index] == null) {
      elementData[index] = new boolean[INSIDE_ARRAY_INIT_SIZE];
    }
  }

  @Override
  protected int getArrayLength() {
    return elementData.length;
  }

  @Override
  protected void growArray(int size) {
    elementData = Arrays.copyOf(elementData, size);
  }

}
