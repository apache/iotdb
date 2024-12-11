package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public class AggregationWindowFunction implements WindowFunction {
  private final WindowAggregator aggregator;
  private int currentStart;
  private int currentEnd;

  public AggregationWindowFunction(WindowAggregator aggregator) {
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
  public void transform(
      Column[] partition,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd) {
    if (frameStart < 0) {
      // Empty frame
      reset();
    } else if (frameStart == currentStart && frameEnd >= currentEnd) {
      // Frame expansion
      Column[] columns = getColumnsRegion(partition, currentEnd + 1, frameEnd);
      aggregator.addInput(columns);
    } else {
      buildNewFrame(partition, frameStart, frameEnd);
    }

    aggregator.evaluate(builder);
  }

  private void buildNewFrame(Column[] partition, int frameStart, int frameEnd) {
    if (aggregator.removable()) {
      int prefix = Math.abs(currentStart - frameStart);
      int suffix = Math.abs(currentEnd - frameEnd);
      int frameLength = frameEnd - frameStart + 1;

      // Compare remove && add cost with re-computation
      if (frameLength > prefix + suffix) {
        if (currentStart < frameStart) {
          Column[] columns = getColumnsRegion(partition, currentStart, frameStart - 1);
          aggregator.removeInput(columns);
        } else if (currentStart > frameStart) {
          Column[] columns = getColumnsRegion(partition, frameStart, currentStart - 1);
          aggregator.addInput(columns);
        } // Do nothing when currentStart == frameStart

        if (frameEnd < currentEnd) {
          Column[] columns = getColumnsRegion(partition, frameEnd + 1, currentEnd);
          aggregator.removeInput(columns);
        } else if (frameEnd > currentEnd) {
          Column[] columns = getColumnsRegion(partition, currentEnd + 1, frameEnd);
          aggregator.addInput(columns);
        } // Do nothing when frameEnd == currentEnd

        currentStart = frameStart;
        currentEnd = frameEnd;
        return;
      }
    }

    // Re-compute
    aggregator.reset();
    Column[] columns = getColumnsRegion(partition, frameStart, frameEnd);
    aggregator.addInput(columns);

    currentStart = frameStart;
    currentEnd = frameEnd;
  }

  // [start, end], i.e., inclusive
  private Column[] getColumnsRegion(Column[] columns, int start, int end) {
    int length = end - start + 1;
    Column[] ret = new Column[columns.length];
    for (int i = 0; i < columns.length; i++) {
      ret[i] = columns[i].getRegion(start, length);
    }

    return ret;
  }

  @Override
  public boolean needPeerGroup() {
    return false;
  }
}
