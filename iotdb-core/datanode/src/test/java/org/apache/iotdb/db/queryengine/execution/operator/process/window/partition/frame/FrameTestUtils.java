package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class FrameTestUtils {
  private final List<ColumnList> sortedColumns;

  private final int partitionStart;
  private final int partitionEnd;

  private int currentGroupIndex = -1;
  private int peerGroupStart;
  private int peerGroupEnd;

  private final RowComparator peerGroupComparator;
  private final Frame frame;

  private final List<Integer> frameStarts;
  private final List<Integer> frameEnds;

  public FrameTestUtils(TsBlock tsBlock, TSDataType inputDataType, FrameInfo frameInfo) {
    this.sortedColumns = tsBlockToColumnLists(tsBlock);
    this.partitionStart = 0;
    this.partitionEnd = tsBlock.getPositionCount();

    this.peerGroupComparator = new RowComparator(Collections.singletonList(inputDataType));

    updatePeerGroup(0);
    this.frame = createFrame(frameInfo);

    this.frameStarts = new ArrayList<>();
    this.frameEnds = new ArrayList<>();
  }

  public void processAllRows() {
    for (int i = partitionStart; i < partitionEnd; i++ ) {
      if (i == peerGroupEnd) {
        updatePeerGroup(i);
      }

      Range range = frame.getRange(i, currentGroupIndex, peerGroupStart, peerGroupEnd);
      this.frameStarts.add(range.getStart());
      this.frameEnds.add(range.getEnd());
    }
  }

  public List<Integer> getFrameStarts() {
    return frameStarts;
  }

  public List<Integer> getFrameEnds() {
    return frameEnds;
  }

  private void updatePeerGroup(int index) {
    currentGroupIndex++;
    peerGroupStart = index;
    // Find end of peer group
    peerGroupEnd = peerGroupStart + 1;
    while (peerGroupEnd < partitionEnd
        && peerGroupComparator.equalColumnLists(sortedColumns, peerGroupStart, peerGroupEnd)) {
      peerGroupEnd++;
    }
  }

  private List<ColumnList> tsBlockToColumnLists(TsBlock tsBlock) {
    Column[] allColumns = tsBlock.getValueColumns();

    List<ColumnList> columnLists = new ArrayList<>();
    for (Column column : allColumns) {
      ColumnList columnList = new ColumnList(Collections.singletonList(column));
      columnLists.add(columnList);
    }

    return columnLists;
  }

  private Frame createFrame(FrameInfo frameInfo) {
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

    return frame;
  }

  public static TsBlock createTsBlockWithInts(int[] inputs) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      columnBuilders[0].writeInt(input);
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(
            TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  public static TsBlock createTsBlockWithIntsAndNulls(int[] inputs) {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int input : inputs) {
      if (input >= 0) {
        columnBuilders[0].writeInt(input);
      } else {
        // Mimic null value
        columnBuilders[0].appendNull();
      }
      tsBlockBuilder.declarePosition();
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(
            TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }
}
