package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.rank;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class RowNumberFunction implements WindowFunction {
  public RowNumberFunction() {}

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
    builder.writeLong(index + 1);
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
