package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.exception.FrameTypeException;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;

public class RowsFrame implements Frame {
  private final FrameInfo frameInfo;
  private final int partitionStart;
  private final int partitionSize;

  public RowsFrame(FrameInfo frameInfo, int partitionStart, int partitionEnd) {
    assert frameInfo.getFrameType() == FrameInfo.FrameType.ROWS;

    this.frameInfo = frameInfo;
    this.partitionStart = partitionStart;
    this.partitionSize = partitionEnd - partitionStart;
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    int posInPartition = currentPosition - partitionStart;

    int offset;
    int frameStart;
    switch (frameInfo.getStartType()) {
      case UNBOUNDED_PRECEDING:
        frameStart = 0;
        break;
      case PRECEDING:
        offset = (int) frameInfo.getStartOffset();
        frameStart = posInPartition - offset;
        break;
      case CURRENT_ROW:
        frameStart = posInPartition;
        break;
      case FOLLOWING:
        offset = (int) frameInfo.getStartOffset();
        frameStart = posInPartition + offset;
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new FrameTypeException(true);
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        offset = (int) frameInfo.getEndOffset();
        frameEnd = posInPartition - offset;
        break;
      case CURRENT_ROW:
        frameEnd = posInPartition;
        break;
      case FOLLOWING:
        offset = (int) frameInfo.getEndOffset();
        frameEnd = posInPartition + offset;
        break;
      case UNBOUNDED_FOLLOWING:
        frameEnd = partitionSize - 1;
        break;
      default:
        // UNBOUND_PRECEDING is not allowed in frame end
        throw new FrameTypeException(false);
    }

    // Empty frame
    if (frameEnd < frameStart || frameEnd < 0 || frameStart >= partitionSize) {
      return new Range(-1, -1);
    }

    frameStart = Math.max(frameStart, 0);
    frameEnd = Math.min(frameEnd, partitionSize - 1);
    return new Range(frameStart, frameEnd);
  }
}
