package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class NthValueFunction implements WindowFunction {
  private final int n;
  private final int channel;

  public NthValueFunction(int n, int channel) {
    this.n = n;
    this.channel = channel;
  }

  @Override
  public void reset() {}

  @Override
  public void transform(
      Column[] partition,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd) {
    // n starts with 1
    int count = partition[0].getPositionCount();
    int offset = frameStart + n - 1;

    if (offset < count) {
      builder.write(partition[channel], offset);
    } else {
      builder.appendNull();
    }
  }
}
