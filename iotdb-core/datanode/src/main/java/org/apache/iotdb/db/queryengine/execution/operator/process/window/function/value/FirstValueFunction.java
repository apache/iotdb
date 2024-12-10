package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class FirstValueFunction implements WindowFunction {
  private final int channel;

  public FirstValueFunction(int channel) {
    this.channel = channel;
  }

  @Override
  public void reset() {}

  @Override
  public void transform(
      Column[] partition,
      ColumnBuilder builder,
      int index,
      int frameStart, int frameEnd,
      int peerGroupStart, int peerGroupEnd) {
    builder.write(partition[channel], frameStart);
  }
}
