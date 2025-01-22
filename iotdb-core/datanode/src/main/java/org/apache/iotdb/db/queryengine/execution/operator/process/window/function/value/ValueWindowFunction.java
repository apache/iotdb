package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.tsfile.block.column.ColumnBuilder;

public abstract class ValueWindowFunction implements WindowFunction {
  @Override
  public void reset() {
    // do nothing, value functions are stateless
  }

  @Override
  public void transform(Partition partition, ColumnBuilder builder, int index, int frameStart, int frameEnd, int peerGroupStart, int peerGroupEnd) {
    transform(partition, builder, index, frameStart, frameEnd);
  }

  public abstract void transform(Partition partition, ColumnBuilder builder, int index, int frameStart, int frameEnd);

  @Override
  public boolean needPeerGroup() {
    return false;
  }
}
