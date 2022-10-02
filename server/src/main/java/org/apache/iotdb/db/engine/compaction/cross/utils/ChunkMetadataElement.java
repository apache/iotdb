package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;

public class ChunkMetadataElement {
  public IChunkMetadata chunkMetadata;

  public int priority;

  public boolean isOverlaped = false;

  public long startTime;

  public boolean isLastChunk;

  public FileElement fileElement;

  public ChunkMetadataElement(
      IChunkMetadata chunkMetadata, int priority, boolean isLastChunk, FileElement fileElement) {
    this.chunkMetadata = chunkMetadata;
    this.priority = priority;
    this.startTime = chunkMetadata.getStartTime();
    this.isLastChunk = isLastChunk;
    this.fileElement = fileElement;
  }
}
