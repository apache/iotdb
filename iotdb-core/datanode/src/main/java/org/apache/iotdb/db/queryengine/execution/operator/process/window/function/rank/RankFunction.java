package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class RankFunction implements WindowFunction {
  private long rank;
  private long count;
  private long currentPeerGroupStart;

  public RankFunction() {
    reset();
  }

  @Override
  public void reset() {
    rank = 0;
    count = 1;
    currentPeerGroupStart = -1;
  }

  @Override
  public void transform(
      Column[] partition,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd) {
    if (currentPeerGroupStart != peerGroupStart) {
      // New peer group
      currentPeerGroupStart = peerGroupStart;
      rank += count;
      count = 1;
    } else {
      count++;
    }

    builder.writeLong(rank);
  }

  @Override
  public boolean needFrame() {
    return false;
  }
}
