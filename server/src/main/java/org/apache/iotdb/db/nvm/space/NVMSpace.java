package org.apache.iotdb.db.nvm.space;

import java.nio.ByteBuffer;

public class NVMSpace {

  protected long offset;
  protected long size;
  protected ByteBuffer byteBuffer;

  NVMSpace(long offset, long size, ByteBuffer byteBuffer) {
    this.offset = offset;
    this.size = size;
    this.byteBuffer = byteBuffer;
  }

  public long getOffset() {
    return offset;
  }

  public long getSize() {
    return size;
  }

  public void put(int index, byte v) {
    byteBuffer.put(index, v);
  }

  public byte get(int index) {
    return byteBuffer.get(index);
  }

  public void putShort(int index, short v) {
    byteBuffer.putShort(index * Short.BYTES, v);
  }

  public short getShort(int index) {
    return byteBuffer.getShort(index * Short.BYTES);
  }

  public void putInt(int index, int v) {
    byteBuffer.putInt(index * Integer.BYTES, v);
  }

  public int getInt(int index) {
    return byteBuffer.getInt(index * Integer.BYTES);
  }

  public void putLong(int index, long v) {
    byteBuffer.putLong(index * Long.BYTES, v);
  }

  public long getLong(int index) {
    return byteBuffer.getLong(index * Long.BYTES);
  }

  public void put(byte[] v) {
    byteBuffer.put(v);
  }

  public void get(byte[] src) {
    byteBuffer.get(src);
  }
}
