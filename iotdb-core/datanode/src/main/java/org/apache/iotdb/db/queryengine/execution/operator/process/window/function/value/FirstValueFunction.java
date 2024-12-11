package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class FirstValueFunction implements WindowFunction {
  private final int channel;
  private final boolean ignoreNull;

  public FirstValueFunction(int channel, boolean ignoreNull) {
    this.channel = channel;
    this.ignoreNull = ignoreNull;
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
    // Empty frame
    if (frameStart < 0) {
      builder.appendNull();
      return;
    }

    if (ignoreNull) {
      // Handle nulls
      int pos = index;
      while (pos <= frameEnd && partition[channel].isNull(pos)) {
        pos++;
      }

      if (pos > frameEnd) {
        builder.appendNull();
      } else {
        builder.write(partition[channel], pos);
      }
    } else {
      builder.write(partition[channel], frameStart);
    }
  }
}
