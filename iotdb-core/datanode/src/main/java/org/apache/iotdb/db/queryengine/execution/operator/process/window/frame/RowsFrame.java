package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

import org.apache.tsfile.read.common.block.TsBlock;

public class RowsFrame implements Frame {
  public RowsFrame(FrameInfo frameInfo, int partitionStart, int partitionEnd, TsBlock tsBlock) {}

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    return null;
  }
}
