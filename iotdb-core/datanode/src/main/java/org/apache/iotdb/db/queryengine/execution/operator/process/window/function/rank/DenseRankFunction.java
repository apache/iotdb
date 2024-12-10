package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class DenseRankFunction implements WindowFunction {
  private long rank;
  private long currentPeerGroupStart;

  public DenseRankFunction() {
    reset();
  }

  @Override
  public void reset() {
    rank = 0;
    currentPeerGroupStart = -1;
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder, int index, int frameStart, int frameEnd, int peerGroupStart, int peerGroupEnd) {
    if (currentPeerGroupStart != peerGroupStart) {
      // New peer group
      currentPeerGroupStart = peerGroupStart;
      rank++;
    }

    builder.writeLong(rank);
  }
}
