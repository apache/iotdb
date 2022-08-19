package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

public class ChunkMetadataElement {
  ChunkMetadata chunkMetadata;
  public int priority;

  public boolean isOverlaped = false;

  public ChunkMetadataElement(ChunkMetadata chunkMetadata, int priority) {
    this.chunkMetadata = chunkMetadata;
    this.priority = priority;
  }

  public long getStartTime() {
    return chunkMetadata.getStartTime();
  }

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }
}
