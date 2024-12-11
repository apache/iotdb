package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class NthValueFunction implements WindowFunction {
  private final int n;
  private final int channel;
  private final boolean ignoreNull;

  public NthValueFunction(int n, int channel, boolean ignoreNull) {
    this.n = n;
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

    int pos;
    if (ignoreNull) {
      // Handle nulls
      pos = index;
      int nonNullCount = 0;
      while (pos <= frameEnd) {
        if (!partition[channel].isNull(pos)) {
          nonNullCount++;
          if (nonNullCount == n) {
            break;
          }
        }
        pos++;
      }
    } else {
      // n starts with 1
      pos = frameStart + n - 1;
    }

    if (pos >= frameStart && pos <= frameEnd) {
      builder.write(partition[channel], pos);
    } else {
      builder.appendNull();
    }
  }
}
