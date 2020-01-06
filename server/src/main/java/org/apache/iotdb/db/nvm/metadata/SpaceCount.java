package org.apache.iotdb.db.nvm.metadata;

import org.apache.iotdb.db.nvm.space.NVMSpace;

public class SpaceCount extends NVMSpaceMetadata {

  public SpaceCount(NVMSpace space) {
    super(space);
  }

  public void put(int v) {
    space.getByteBuffer().putInt(0, v);
  }

  public int get() {
    return space.getByteBuffer().getInt(0);
  }
}
