package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

import org.apache.tsfile.read.common.block.TsBlock;

public class RowsFrame implements Frame {
  private final FrameInfo frameInfo;
  private int partitionStart;
  private int partitionEnd;
  private final TsBlock tsBlock;

  public RowsFrame(FrameInfo frameInfo, int partitionStart, int partitionEnd, TsBlock tsBlock) {
    this.frameInfo = frameInfo;
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.tsBlock = tsBlock;
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    int frameStart = -1;
    int frameEnd = -1;

    if (frameInfo.getStartType() == FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING) {
      frameStart = 0;
    }

    if (frameInfo.getEndType() == FrameInfo.FrameBoundType.CURRENT_ROW) {
      frameEnd = currentPosition - partitionStart;
    }

    return new Range(frameStart, frameEnd);
  }
}
