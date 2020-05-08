package org.apache.iotdb.db.engine.merge.sizeMerge;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

public enum MergeSizeSelectorStrategy {
  TIME_RANGE,
  POINT_RANGE;

  public boolean isChunkEnoughLarge(IChunkWriter chunkWriter, long minChunkPointNum, long minTime,
      long maxTime, long timeBlock) {
    switch (this) {
      case TIME_RANGE:
        return maxTime - minTime >= timeBlock;
      case POINT_RANGE:
      default:
        return chunkWriter.getPtNum() >= minChunkPointNum;
    }
  }

  public boolean isChunkEnoughLarge(ChunkMetadata chunkMetadata, long minChunkPointNum,
      long timeBlock) {
    switch (this) {
      case TIME_RANGE:
        return chunkMetadata.getEndTime() - chunkMetadata.getStartTime() >= timeBlock;
      case POINT_RANGE:
      default:
        return chunkMetadata.getNumOfPoints() >= minChunkPointNum;
    }
  }
}
