package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

public interface WindowFunction {
  void reset();

  void processRow(
      ColumnBuilder builder, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd);
}
