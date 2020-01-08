package org.apache.iotdb.db.nvm.metadata;

import org.apache.iotdb.db.nvm.space.NVMSpace;

public class OffsetMemo extends NVMSpaceMetadata {

  public OffsetMemo(NVMSpace space) {
    super(space);
  }

  public void set(int index, long offset) {
    space.getByteBuffer().putLong(index, offset);
  }

  public long get(int index) {
    return space.getByteBuffer().getLong(index);
  }
}
