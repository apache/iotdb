package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.tsfile.block.column.ColumnBuilder;

public abstract class RankWindowFunction implements WindowFunction {
  private int currentPeerGroupStart;

  @Override
  public void reset() {
    currentPeerGroupStart = 0;
  }

  @Override
  public void transform(
      Partition partition,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd) {
    boolean isNewPeerGroup = false;
    if (peerGroupStart != currentPeerGroupStart) {
      currentPeerGroupStart = peerGroupStart;
      isNewPeerGroup = true;
    }

    int peerGroupCount = (peerGroupEnd - peerGroupStart) + 1;

    transform(partition, builder, index, isNewPeerGroup, peerGroupCount);
  }

  public abstract void transform(Partition partition, ColumnBuilder builder, int index, boolean isNewPeerGroup, int peerGroupCount);

  @Override
  public boolean needFrame() {
    return false;
  }
}
