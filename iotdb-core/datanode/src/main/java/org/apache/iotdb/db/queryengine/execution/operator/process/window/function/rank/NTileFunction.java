package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class NTileFunction implements WindowFunction {
  private final int n;

  public NTileFunction(int n) {
    this.n = n;
  }

  @Override
  public void reset() {}

  @Override
  public void transform(
      Partition partition,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd) {
    builder.writeLong(bucket(n, index, partition.getPositionCount()) + 1);
  }

  private long bucket(long buckets, int index, int count) {
    if (count < buckets) {
      return index;
    }

    long remainderRows = count % buckets;
    long rowsPerBucket = count / buckets;

    if (index < ((rowsPerBucket + 1) * remainderRows)) {
      return index / (rowsPerBucket + 1);
    }

    return (index - remainderRows) / rowsPerBucket;
  }

  @Override
  public boolean needPeerGroup() {
    return false;
  }

  @Override
  public boolean needFrame() {
    return false;
  }
}
