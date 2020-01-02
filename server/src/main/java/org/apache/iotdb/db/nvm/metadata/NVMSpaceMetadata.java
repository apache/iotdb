package org.apache.iotdb.db.nvm.metadata;

import java.nio.ByteBuffer;

public abstract class NVMSpaceMetadata {

  protected ByteBuffer byteBuffer;

  public NVMSpaceMetadata(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }
}
