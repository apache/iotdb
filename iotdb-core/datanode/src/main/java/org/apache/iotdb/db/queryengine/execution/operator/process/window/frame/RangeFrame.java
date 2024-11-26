package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

import org.apache.tsfile.read.common.block.TsBlock;

public class RangeFrame implements Frame {
  public RangeFrame(
      FrameInfo frameInfo,
      int partitionStart,
      int partitionEnd,
      TsBlock tsBlock,
      Frame.Range range) {}

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    return null;
  }
}
