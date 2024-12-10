package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.Frame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.GroupsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.RangeFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.RowsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;

public final class Partition {
  private final TsBlock tsBlock;
  private final int partitionStart;
  private final int partitionEnd;

  private final WindowFunction windowFunction;

  private int peerGroupStart;
  private int peerGroupEnd;
  private List<Column> frameColumns = new ArrayList<>();
  private RowComparator peerGroupComparator;

  private int currentGroupIndex = -1;
  private int currentPosition;

  private final Frame frame;

  public Partition(
      TsBlock tsBlock,
      List<TSDataType> dataTypes,
      int partitionStart,
      int partitionEnd,
      WindowFunction windowFunction,
      FrameInfo frameInfo,
      List<Integer> sortChannels) {
    this.tsBlock = tsBlock;
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.windowFunction = windowFunction;
    // Prepare for peer group comparing
    List<TSDataType> sortDataTypes = new ArrayList<>();
    for (int channel : sortChannels) {
      TSDataType dataType = dataTypes.get(channel);
      sortDataTypes.add(dataType);
    }
    this.peerGroupComparator = new RowComparator(sortDataTypes);
    for (Integer channel : sortChannels) {
      Column partitionColumn = tsBlock.getColumn(channel);
      frameColumns.add(partitionColumn);
    }

    // Reset functions for new partition
    windowFunction.reset();

    currentPosition = partitionStart;
    updatePeerGroup();

    switch (frameInfo.getFrameType()) {
      case RANGE:
        if (frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
          frame =
              new RangeFrame(
                  frameInfo,
                  partitionStart,
                  partitionEnd,
                  frameColumns,
                  peerGroupComparator,
                  new Range(0, partitionEnd - partitionStart - 1));
        } else {
          frame =
              new RangeFrame(
                  frameInfo,
                  partitionStart,
                  partitionEnd,
                  frameColumns,
                  peerGroupComparator,
                  new Range(0, peerGroupEnd - partitionStart - 1));
        }
        break;
      case ROWS:
        frame = new RowsFrame(frameInfo, partitionStart, partitionEnd);
        break;
      case GROUPS:
        frame =
            new GroupsFrame(
                frameInfo,
                partitionStart,
                partitionEnd,
                frameColumns,
                peerGroupComparator,
                peerGroupEnd - partitionStart - 1);
        break;
      default:
        throw new UnsupportedOperationException("not yet implemented");
    }
  }

  public int getPartitionStart() {
    return partitionStart;
  }

  public int getPartitionEnd() {
    return partitionEnd;
  }

  public boolean hasNext() {
    return currentPosition < partitionEnd;
  }

  public void processNextRow(TsBlockBuilder builder) {
    // Copy origin data
    int count = tsBlock.getValueColumnCount();
    for (int i = 0; i < count; i++) {
      Column column = tsBlock.getColumn(i);
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      columnBuilder.write(column, currentPosition);
    }

    if (currentPosition == peerGroupEnd) {
      updatePeerGroup();
    }

    Range range =
        frame.getRange(currentPosition, currentGroupIndex, peerGroupStart, peerGroupEnd);
    windowFunction.processRow(
        builder.getColumnBuilder(count),
        peerGroupStart - partitionStart,
        peerGroupEnd - partitionStart - 1,
        range.getStart(),
        range.getEnd());

    currentPosition++;
    builder.declarePosition();
  }

  private void updatePeerGroup() {
    currentGroupIndex++;
    peerGroupStart = currentPosition;
    // find end of peer group
    peerGroupEnd = peerGroupStart + 1;
    while ((peerGroupEnd < partitionEnd)) {
      peerGroupEnd++;
    }
  }
}
