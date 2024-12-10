package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public interface WindowFunction {
  void reset();

  void transform(
      Column[] columns,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd);
}
