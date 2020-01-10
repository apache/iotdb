package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;

public class Counter extends NVMSpaceMetadata {

  public Counter() throws IOException {
  }

  public void increment() {
    put(get() + 1);
  }

  public void decrement() {
    put(get() - 1);
  }

  private void put(int v) {
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
