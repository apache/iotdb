package org.apache.iotdb.db.nvm.metadata;

import org.apache.iotdb.db.nvm.space.NVMSpace;

public abstract class NVMSpaceMetadata {

  protected NVMSpace space;

  public NVMSpaceMetadata(NVMSpace space) {
    this.space = space;
  }
}
