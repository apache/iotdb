package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

public class ChunkMetadataElement {
  public ChunkMetadata chunkMetadata;

  public int priority;

  public boolean isOverlaped = false;

  public long startTime;

  public boolean isFirstChunk = false;

  public ChunkMetadataElement(ChunkMetadata chunkMetadata, int priority) {
    this.chunkMetadata = chunkMetadata;
    this.priority = priority;
    this.startTime = chunkMetadata.getStartTime();
  }
}
