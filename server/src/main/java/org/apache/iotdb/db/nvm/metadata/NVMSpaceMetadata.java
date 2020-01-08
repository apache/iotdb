package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import org.apache.iotdb.db.nvm.space.NVMSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;

public abstract class NVMSpaceMetadata {

  protected NVMSpace space;

  public NVMSpaceMetadata() throws IOException {
    long size = getUnitSize() * getUnitNum();
    space = NVMSpaceManager.getInstance().allocateSpace(size);
  }

  abstract int getUnitSize();

  abstract int getUnitNum();
}
