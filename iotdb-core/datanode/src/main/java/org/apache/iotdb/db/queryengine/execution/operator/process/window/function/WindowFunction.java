package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.tsfile.block.column.ColumnBuilder;

public interface WindowFunction {
  void reset();

  void processRow(
      ColumnBuilder builder, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd);
}
