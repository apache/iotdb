package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class LeadFunction implements WindowFunction {
  private final int channel;
  private final Integer offset;
  private final Integer defaultVal;

  public LeadFunction(int channel, Integer offset, Integer defaultVal) {
    this.channel = channel;
    this.offset = offset == null ? 1 : offset;
    this.defaultVal = defaultVal;
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
    int pos = index + offset;
    if (pos < partition[0].getPositionCount()) {
      builder.write(partition[channel], pos);
    } else if (defaultVal != null) {
      // TODO: Replace write object
      builder.writeObject(defaultVal);
    } else {
      builder.appendNull();
    }
  }
}
