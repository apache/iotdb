package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;

public class FileElement {
    public TsFileResource resource;

  public MetadataIndexNode firstMeasurementNode = null;

    public boolean isOverlap=false;

    public FileElement(TsFileResource resource) {
        this.resource = resource;
    }

  public FileElement(TsFileResource resource, MetadataIndexNode firstMeasurementNode) {
    this.resource = resource;
    this.firstMeasurementNode = firstMeasurementNode;
  }
}
