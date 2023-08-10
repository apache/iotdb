package org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader;

public class TaskSummary {
  private long overlapChunk;
  private long overlapChunkGroup;
  private long totalChunks;
  private long totalChunkGroups;

  public long getOverlapChunk() {
    return overlapChunk;
  }

  public void setOverlapChunk(long overlapChunk) {
    this.overlapChunk = overlapChunk;
  }

  public long getOverlapChunkGroup() {
    return overlapChunkGroup;
  }

  public void setOverlapChunkGroup(long overlapChunkGroup) {
    this.overlapChunkGroup = overlapChunkGroup;
  }

  public long getTotalChunks() {
    return totalChunks;
  }

  public void setTotalChunks(long totalChunks) {
    this.totalChunks = totalChunks;
  }

  public long getTotalChunkGroups() {
    return totalChunkGroups;
  }

  public void setTotalChunkGroups(long totalChunkGroups) {
    this.totalChunkGroups = totalChunkGroups;
  }
}
