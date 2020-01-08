package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;

public class SpaceCount extends NVMSpaceMetadata {

  public SpaceCount() throws IOException {
  }

  public void put(int v) {
    space.putInt(0, v);
  }

  public int get() {
    return space.getInt(0);
  }

  @Override
  int getUnitSize() {
    return Integer.BYTES;
  }

  @Override
  int getUnitNum() {
    return 1;
  }
}
