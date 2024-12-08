package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.Frame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.GroupsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.RangeFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.RowsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import static org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;

public final class Partition {
  private final TsBlock tsBlock;
  private final int partitionStart;
  private final int partitionEnd;

  private final WindowFunction windowFunction;

  private int peerGroupStart;
  private int peerGroupEnd;

  private int currentGroupIndex = -1;
  private int currentPosition;

  private final Frame frame;

  public Partition(
      TsBlock tsBlock,
      int partitionStart,
      int partitionEnd,
      WindowFunction windowFunction,
      FrameInfo frameInfo) {
    this.tsBlock = tsBlock;
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.windowFunction = windowFunction;

    // reset functions for new partition
    windowFunction.reset();

    currentPosition = partitionStart;
    updatePeerGroup();

    switch (frameInfo.getFrameType()) {
      case RANGE:
        if (frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
          frame =
              new RangeFrame(
                  frameInfo,
                  partitionStart,
                  partitionEnd,
                  tsBlock,
                  new Frame.Range(0, partitionEnd - partitionStart - 1));
        } else {
          frame =
              new RangeFrame(
                  frameInfo,
                  partitionStart,
                  partitionEnd,
                  tsBlock,
                  new Frame.Range(0, peerGroupEnd - partitionStart - 1));
        }
        break;
      case ROWS:
        frame = new RowsFrame(frameInfo, partitionStart, partitionEnd, tsBlock);
        break;
      case GROUPS:
        frame =
            new GroupsFrame(
                frameInfo,
                partitionStart,
                partitionEnd,
                tsBlock,
                peerGroupEnd - partitionStart - 1);
        break;
      default:
        throw new UnsupportedOperationException("not yet implemented");
    }
  }

  public int getPartitionStart() {
    return partitionStart;
  }

  public int getPartitionEnd() {
    return partitionEnd;
  }

  public boolean hasNext() {
    return currentPosition < partitionEnd;
  }

  public void processNextRow(TsBlockBuilder builder) {
    // Copy origin data
    int count = tsBlock.getValueColumnCount();
    for (int i = 0; i < count; i++) {
      Object object = tsBlock.getColumn(i).getObject(currentPosition);
      builder.getColumnBuilder(i).writeObject(object);
    }

    if (currentPosition == peerGroupEnd) {
      updatePeerGroup();
    }

    Frame.Range range =
        frame.getRange(currentPosition, currentGroupIndex, peerGroupStart, peerGroupEnd);
    windowFunction.processRow(
        builder.getColumnBuilder(count),
        peerGroupStart - partitionStart,
        peerGroupEnd - partitionStart - 1,
        range.getStart(),
        range.getEnd());

    currentPosition++;
    builder.declarePosition();
  }

  private void updatePeerGroup() {
    //    currentGroupIndex++;
    //    peerGroupStart = currentPosition;
    //    // find end of peer group
    //    peerGroupEnd = peerGroupStart + 1;
    //    while ((peerGroupEnd < partitionEnd)) {
    //      peerGroupEnd++;
    //    }
  }
}
