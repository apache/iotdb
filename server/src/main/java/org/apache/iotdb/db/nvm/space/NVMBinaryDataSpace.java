package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class NVMBinaryDataSpace extends NVMDataSpace {

  private int cacheSize;
  private Binary[] cachedBinaries;

  NVMBinaryDataSpace(long offset, long size, ByteBuffer byteBuffer, int index, boolean recover) {
    super(offset, size, byteBuffer, index, TSDataType.TEXT, false);

    cacheSize = 0;
    cachedBinaries = new Binary[ARRAY_SIZE];
    if (recover) {
      recoverCache();
    }
  }

  private void recoverCache() {
    int size = byteBuffer.getInt();
    cacheSize = size;
    for (int i = 0; i < size; i++) {
      int len = byteBuffer.getInt();
      byte[] bytes = new byte[len];
      byteBuffer.get(bytes);
      cachedBinaries[i] = new Binary(bytes);
    }
  }

  @Override
  public int getUnitNum() {
    return cachedBinaries.length;
  }

  @Override
  public Object getData(int index) {
    return cachedBinaries[index];
  }

  @Override
  public void setData(int index, Object object) {
    // todo nos support index
    Binary binary = (Binary) object;
    cachedBinaries[index] = binary;
    if (index >= cacheSize) {
      byteBuffer.putInt(0, index);
      cacheSize = index;
    }
    byteBuffer.putInt(binary.getLength());
    byteBuffer.put(binary.getValues());
  }

  @Override
  public Object toArray() {
    return cachedBinaries;
  }
}
