package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.tsfile.block.column.ColumnBuilder;

public class FirstValueFunction implements WindowFunction {
  private final int channel;

  public FirstValueFunction(int channel) {
    this.channel = channel;
  }

  @Override
  public void reset() {}

  @Override
  public void processRow(
      ColumnBuilder builder, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd) {
    builder.writeInt(frameStart);
  }
}
