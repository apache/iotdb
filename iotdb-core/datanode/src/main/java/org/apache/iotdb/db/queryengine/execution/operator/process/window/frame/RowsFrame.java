package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

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

    int frameStart = -1;
    if (frameInfo.getStartType() == FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING) {
      frameStart = 0;
    } else if (frameInfo.getStartType() == FrameInfo.FrameBoundType.PRECEDING) {
      int offset = (int) frameInfo.getStartOffset();
      frameStart = Math.max(posInPartition - offset, 0);
    } else if (frameInfo.getStartType() == FrameInfo.FrameBoundType.CURRENT_ROW) {
      frameStart = posInPartition;
    } else if (frameInfo.getStartType() == FrameInfo.FrameBoundType.FOLLOWING) {
      int offset = (int) frameInfo.getStartOffset();
      frameStart = Math.min(posInPartition + offset, partitionSize);
    } // UNBOUND_FOLLOWING is not allowed in frame start

    int frameEnd = -1;
    if (frameInfo.getEndType() == FrameInfo.FrameBoundType.PRECEDING) {
      int offset = (int) frameInfo.getEndOffset();
      frameEnd = Math.max(posInPartition - offset, 0);
    } else if (frameInfo.getEndType() == FrameInfo.FrameBoundType.CURRENT_ROW) {
      frameEnd = posInPartition;
    } else if (frameInfo.getEndType() == FrameInfo.FrameBoundType.FOLLOWING) {
      int offset = (int) frameInfo.getEndOffset();
      frameEnd = Math.min(posInPartition + offset, partitionSize - 1);
    } else if (frameInfo.getEndType() == FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING) {
      frameEnd = partitionSize - 1;
    } // UNBOUND_PRECEDING is not allowed in frame end

    return new Range(frameStart, frameEnd);
  }
}
