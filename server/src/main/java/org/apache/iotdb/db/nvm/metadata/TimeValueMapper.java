package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;

public class TimeValueMapper extends NVMSpaceMetadata {

  public TimeValueMapper() throws IOException {
  }

  public void map(int timeSpaceIndex, int valueSpaceIndex) {
    space.putInt(timeSpaceIndex, valueSpaceIndex);
  }

  public int get(int timeSpaceIndex) {
    return space.getInt(timeSpaceIndex);
  }

  @Override
  int getUnitSize() {
    return Integer.BYTES;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
