package org.apache.iotdb.consensus.common;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

public class SnapshotMeta {
  private ByteBuffer metadata;
  private List<File> snapshotFiles;

  public SnapshotMeta(ByteBuffer metadata, List<File> snapshotFiles) {
    this.metadata = metadata;
    this.snapshotFiles = snapshotFiles;
  }

  public ByteBuffer getMetadata() {
    return metadata;
  }

  public void setMetadata(ByteBuffer metadata) {
    this.metadata = metadata;
  }

  public List<File> getSnapshotFiles() {
    return snapshotFiles;
  }

  public void setSnapshotFiles(List<File> snapshotFiles) {
    this.snapshotFiles = snapshotFiles;
  }
}
