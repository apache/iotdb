package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;

import org.apache.tsfile.block.column.ColumnBuilder;

public class LagFunction implements WindowFunction {
  private final int channel;
  private final Integer offset;
  private final Object defaultVal;
  private final boolean ignoreNull;

  public LagFunction(int channel, Integer offset, Object defaultVal, boolean ignoreNull) {
    this.channel = channel;
    this.offset = offset == null ? 1 : offset;
    this.defaultVal = defaultVal;
    this.ignoreNull = ignoreNull;
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
    int pos;
    if (ignoreNull) {
      int nonNullCount = 0;
      pos = index - 1;
      while (pos >= 0) {
        if (!partition.isNull(channel, pos)) {
          nonNullCount++;
          if (nonNullCount == offset) {
            break;
          }
        }

        pos--;
      }
    } else {
      pos = index - offset;
    }

    if (pos >= 0) {
      if (!partition.isNull(channel, pos)) {
        partition.writeTo(builder, channel, pos);
      } else {
        builder.appendNull();
      }
    } else if (defaultVal != null) {
      // TODO: Replace write object
      builder.writeObject(defaultVal);
    } else {
      builder.appendNull();
    }
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
