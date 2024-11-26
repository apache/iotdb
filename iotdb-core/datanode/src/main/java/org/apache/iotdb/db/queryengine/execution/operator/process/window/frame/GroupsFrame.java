package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

import org.apache.tsfile.read.common.block.TsBlock;

public class GroupsFrame implements Frame {
  public GroupsFrame(
      FrameInfo frameInfo, int partitionStart, int partitionEnd, TsBlock tsBlock, int i) {}

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    return null;
  }
}
