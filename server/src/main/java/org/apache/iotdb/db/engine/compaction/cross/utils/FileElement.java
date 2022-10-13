package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

public class FileElement {
  public TsFileResource resource;

  public boolean isOverlap = false;

  public FileElement(TsFileResource resource) {
    this.resource = resource;
  }
}
