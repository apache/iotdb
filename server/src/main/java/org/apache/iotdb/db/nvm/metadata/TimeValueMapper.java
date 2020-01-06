package org.apache.iotdb.db.nvm.metadata;

import org.apache.iotdb.db.nvm.space.NVMSpace;

public class TimeValueMapper extends NVMSpaceMetadata {

  public TimeValueMapper(NVMSpace space) {
    super(space);
  }

  public void map(int timeSpaceIndex, int valueSpaceIndex) {
    space.getByteBuffer().putInt(timeSpaceIndex, valueSpaceIndex);
  }
}
