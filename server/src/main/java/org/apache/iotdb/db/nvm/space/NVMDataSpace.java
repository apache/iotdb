package org.apache.iotdb.db.nvm.space;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMDataSpace extends NVMSpace {

  private final long INVALID_VALUE = 0;

  private int index;
  private TSDataType dataType;
  private int unitSize;

  NVMDataSpace(long offset, long size, ByteBuffer byteBuffer, int index, TSDataType dataType) {
    super(offset, size, byteBuffer);
    this.index = index;
    this.dataType = dataType;
    unitSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
  }

  public int getUnitNum() {
    return (int) (size / unitSize);
  }
  
  public void refreshData() {
    // TODO only for Long
    for (int i = 0; i < size / NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT64); i++) {
      byteBuffer.putLong(i, INVALID_VALUE);
    }
  }

  /**
   * for Long only
   * @return
   */
  public int getValidUnitNum() {
    int count = 0;
    while (true) {
      long v = byteBuffer.getLong(count);
      if (v == INVALID_VALUE) {
        break;
      }

      count++;
    }
    return count;
  }

  public Object get(int index) {
    index *= unitSize;
    Object object = null;
    switch (dataType) {
      case BOOLEAN:
        object = byteBuffer.get(index);
        break;
      case INT32:
        object = byteBuffer.getInt(index);
        break;
      case INT64:
        object = byteBuffer.getLong(index);
        break;
      case FLOAT:
        object = byteBuffer.getFloat(index);
        break;
      case DOUBLE:
        object = byteBuffer.getDouble(index);
        break;
      case TEXT:
        // TODO
        break;
    }
    return object;
  }

  public void set(int index, Object object) {
    index *= unitSize;
    switch (dataType) {
      case BOOLEAN:
        byteBuffer.put(index, (byte) object);
        break;
      case INT32:
        byteBuffer.putInt(index, (int) object);
        break;
      case INT64:
        byteBuffer.putLong(index, (long) object);
        break;
      case FLOAT:
        byteBuffer.putFloat(index, (float) object);
        break;
      case DOUBLE:
        byteBuffer.putDouble(index, (double) object);
        break;
      case TEXT:
        // TODO
        break;
    }
  }

  public int getIndex() {
    return index;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public Object toArray() {
    int arraySize = (int) (size / NVMSpaceManager.getPrimitiveTypeByteSize(dataType));
    Object[] array = new Object[arraySize];
    for (int i = 0; i < arraySize; i++) {
      array[i] = get(i);
    }
    return array;
  }
}
