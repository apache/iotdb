package org.apache.iotdb.tsfile.read.common;

import java.util.Arrays;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * @Author: LiuDaWei
 * @Create: 2020年03月06日
 */
public class LongList extends IoTDBArrayList {

  private long[][] elementData = new long[ARRAY_INIT_SIZE][];

  public LongList() {
    initInsideArray(0);
  }

  @Override
  public void put(long value) {
    ensureCapacityInternal();
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }

  @Override
  public void fastPut(long value) {
    if (currentInsideIndex == INSIDE_ARRAY_INIT_SIZE) {
      currentArrayIndex++;
      currentInsideIndex = 0;
    }
    elementData[currentArrayIndex][currentInsideIndex++] = value;
    size++;
  }

  public long getOriginValue(int index) {
    return elementData[index / INSIDE_ARRAY_INIT_SIZE][index & (INSIDE_ARRAY_INIT_SIZE - 1)];
  }

  @Override
  public Object getValue(int currentReadIndex) {
    return elementData[currentReadIndex / INSIDE_ARRAY_INIT_SIZE]
        [currentReadIndex & (INSIDE_ARRAY_INIT_SIZE - 1)];
  }

  @Override
  protected void initInsideArray(int index) {
    if (elementData[index] == null) {
      elementData[index] = new long[INSIDE_ARRAY_INIT_SIZE];
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
