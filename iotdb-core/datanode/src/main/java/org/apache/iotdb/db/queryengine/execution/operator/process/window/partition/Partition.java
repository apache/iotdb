package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition;

import com.google.common.collect.ImmutableList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.Frame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.GroupsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.RangeFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.RowsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;

public final class Partition {
  private final TsBlock tsBlock;
  private final int partitionStart;
  private final int partitionEnd;
  private final Column[] partition;

  private final List<WindowFunction> windowFunctions;

  private final List<Column> sortedColumns;
  private final RowComparator peerGroupComparator;
  private int peerGroupStart;
  private int peerGroupEnd;

  private int currentGroupIndex = -1;
  private int currentPosition;

  private final List<Frame> frames;

  private final boolean needPeerGroup;

  public Partition(
      TsBlock tsBlock,
      List<TSDataType> dataTypes,
      int partitionStart,
      int partitionEnd,
      List<WindowFunction> windowFunctions,
      List<FrameInfo> frameInfoList,
      List<Integer> sortChannels) {
    this.tsBlock = tsBlock;
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.partition =
        tsBlock.getRegion(partitionStart, partitionEnd - partitionStart).getAllColumns();
    this.windowFunctions = ImmutableList.copyOf(windowFunctions);
    this.frames = new ArrayList<>();

    // Prepare for peer group comparing
    List<TSDataType> sortDataTypes = new ArrayList<>();
    for (int channel : sortChannels) {
      TSDataType dataType = dataTypes.get(channel);
      sortDataTypes.add(dataType);
    }
    peerGroupComparator = new RowComparator(sortDataTypes);
    sortedColumns = new ArrayList<>();
    for (Integer channel : sortChannels) {
      Column partitionColumn = tsBlock.getColumn(channel);
      sortedColumns.add(partitionColumn);
    }

    // Reset functions for new partition
    for (WindowFunction windowFunction : windowFunctions) {
      windowFunction.reset();
    }

    currentPosition = partitionStart;
    updatePeerGroup();

    for (FrameInfo frameInfo : frameInfoList) {
      Frame frame;
      switch (frameInfo.getFrameType()) {
        case RANGE:
          if (frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
            frame =
                new RangeFrame(
                    frameInfo,
                    partitionStart,
                    partitionEnd,
                    sortedColumns,
                    peerGroupComparator,
                    partitionEnd - partitionStart - 1);
          } else {
            frame =
                new RangeFrame(
                    frameInfo,
                    partitionStart,
                    partitionEnd,
                    sortedColumns,
                    peerGroupComparator,
                    peerGroupEnd - partitionStart - 1);
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
                  sortedColumns,
                  peerGroupComparator,
                  peerGroupEnd - partitionStart - 1);
          break;
        default:
          // Unreachable
          throw new UnsupportedOperationException("Unreachable!");
      }
      frames.add(frame);
    }

    needPeerGroup = windowFunctions.stream().anyMatch(WindowFunction::needPeerGroup);
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

    if (needPeerGroup && currentPosition == peerGroupEnd) {
      updatePeerGroup();
    }

    for (int i = 0; i < windowFunctions.size(); i++) {
      Frame frame = frames.get(i);
      WindowFunction windowFunction = windowFunctions.get(i);

      Range frameRange = windowFunction.needFrame()? frame.getRange(currentPosition, currentGroupIndex, peerGroupStart, peerGroupEnd) : new Range(-1, -1);
      windowFunction.transform(
          partition,
          builder.getColumnBuilder(count),
          currentPosition - partitionStart,
          frameRange.getStart(),
          frameRange.getEnd(),
          peerGroupStart - partitionStart,
          peerGroupEnd - partitionStart - 1);

      count++;
    }

    currentPosition++;
    builder.declarePosition();
  }

  private void updatePeerGroup() {
    currentGroupIndex++;
    peerGroupStart = currentPosition;
    // Find end of peer group
    peerGroupEnd = peerGroupStart + 1;
    while (peerGroupEnd < partitionEnd
        && peerGroupComparator.equal(sortedColumns, peerGroupStart, peerGroupEnd)) {
      peerGroupEnd++;
    }
  }
}
