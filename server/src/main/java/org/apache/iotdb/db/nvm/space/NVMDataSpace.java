package org.apache.iotdb.db.nvm.space;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMDataSpace extends NVMSpace {

  private int index;
  private TSDataType dataType;

  NVMDataSpace(long offset, long size, ByteBuffer byteBuffer, int index, TSDataType dataType) {
    super(offset, size, byteBuffer);
    this.index = index;
    this.dataType = dataType;
  }

  @Override
  public NVMDataSpace clone() {
    return new NVMDataSpace(offset, size, cloneByteBuffer(), index, dataType);
  }

  public ByteBuffer cloneByteBuffer() {
    ByteBuffer clone = ByteBuffer.allocate(byteBuffer.capacity());
    byteBuffer.rewind();
    clone.put(byteBuffer);
    byteBuffer.rewind();
    clone.flip();
    return clone;
  }

  public Object get(int index) {
    int objectSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
    index *= objectSize;
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
    int objectSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
    index *= objectSize;
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
