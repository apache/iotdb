package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class AggregationWindowFunction implements WindowFunction {
  private final TableAggregator aggregator;
  private int currentStart;
  private int currentEnd;

  public AggregationWindowFunction(TableAggregator aggregator) {
    this.aggregator = aggregator;
    reset();
  }

  @Override
  public void reset() {
    aggregator.reset();
    currentStart = -1;
    currentEnd = -1;
  }

  @Override
  public void transform(Column[] partition, ColumnBuilder builder, int index, int frameStart, int frameEnd, int peerGroupStart, int peerGroupEnd) {
    if (frameStart < 0) {
      // Empty frame
      reset();
    } else if (frameStart == currentStart && frameEnd >= currentEnd) {
      // Frame expansion
      Column[] columns = getColumnsRegion(partition, currentEnd + 1, frameEnd);
      aggregator.processColumns(columns);
    } else {
      buildNewFrame(frameStart, frameEnd);
    }

    aggregator.evaluate(builder);
  }

  private void buildNewFrame(int frameStart, int frameEnd) {

  }

  private Column[] getColumnsRegion(Column[] columns, int start, int end) {
    int length = end - start + 1;
    Column[] ret = new Column[columns.length];
    for (int i = 0; i < columns.length; i++) {
      ret[i] = columns[i].getRegion(start, length);
    }

    return ret;
  }
}
