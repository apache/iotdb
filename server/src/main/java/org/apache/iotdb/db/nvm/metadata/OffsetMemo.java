package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;

public class OffsetMemo extends NVMSpaceMetadata {

  public OffsetMemo() throws IOException {
  }

  public void set(int index, long offset) {
    space.putLong(index, offset);
  }

  public long get(int index) {
    return space.getLong(index);
  }

  @Override
  int getUnitSize() {
    return Long.BYTES;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
