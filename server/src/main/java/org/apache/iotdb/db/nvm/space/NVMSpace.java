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

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }
}
