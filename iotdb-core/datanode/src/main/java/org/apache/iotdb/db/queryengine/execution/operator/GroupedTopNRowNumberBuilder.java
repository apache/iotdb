package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Iterator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

public class GroupedTopNRowNumberBuilder implements GroupedTopNBuilder {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedTopNRowNumberBuilder.class);

  private final List<TSDataType> sourceTypes;
  private final boolean produceRowNumber;
  private final int[] groupByChannels;
  private final GroupByHash groupByHash;
  private final RowReferencePageManager pageManager = new RowReferencePageManager();
  private final GroupedTopNRowNumberAccumulator groupedTopNRowNumberAccumulator;
  private final TsBlockWithPositionComparator comparator;

  public GroupedTopNRowNumberBuilder(
      List<TSDataType> sourceTypes,
      TsBlockWithPositionComparator comparator,
      int topN,
      boolean produceRowNumber,
      int[] groupByChannels,
      GroupByHash groupByHash) {
    this.sourceTypes = sourceTypes;
    this.produceRowNumber = produceRowNumber;
    this.groupByChannels = groupByChannels;
    this.groupByHash = groupByHash;
    this.comparator = comparator;

    this.groupedTopNRowNumberAccumulator =
        new GroupedTopNRowNumberAccumulator(
            (leftRowId, rightRowId) -> {
              TsBlock leftTsBlock = pageManager.getPage(leftRowId);
              int leftPosition = pageManager.getPosition(leftRowId);
              TsBlock rightTsBlock = pageManager.getPage(rightRowId);
              int rightPosition = pageManager.getPosition(rightRowId);
              return comparator.compareTo(leftTsBlock, leftPosition, rightTsBlock, rightPosition);
            },
            topN,
            pageManager::dereference);
  }

  @Override
  public void addTsBlock(TsBlock tsBlock) {
    int[] groupIds = groupByHash.getGroupIds(tsBlock.getColumns(groupByChannels));
    int groupCount = groupByHash.getGroupCount();

    processTsBlock(tsBlock, groupCount, groupIds);
  }

  @Override
  public Iterator<TsBlock> getResult() {
    return new ResultIterator();
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return INSTANCE_SIZE
        + groupByHash.getEstimatedSize()
        + pageManager.sizeOf()
        + groupedTopNRowNumberAccumulator.sizeOf();
  }

  private void processTsBlock(TsBlock newTsBlock, int groupCount, int[] groupIds) {
    int firstPositionToAdd =
        groupedTopNRowNumberAccumulator.findFirstPositionToAdd(
            newTsBlock, groupCount, groupIds, comparator, pageManager);
    if (firstPositionToAdd < 0) {
      return;
    }

    try (RowReferencePageManager.LoadCursor loadCursor =
        pageManager.add(newTsBlock, firstPositionToAdd)) {
      for (int position = firstPositionToAdd;
          position < newTsBlock.getPositionCount();
          position++) {
        int groupId = groupIds[position];
        loadCursor.advance();
        groupedTopNRowNumberAccumulator.add(groupId, loadCursor);
      }
    }

    pageManager.compactIfNeeded();
  }

  private class ResultIterator extends AbstractIterator<TsBlock> {
    private final TsBlockBuilder pageBuilder;
    private final int groupIdCount = groupByHash.getGroupCount();
    private int currentGroupId = -1;
    private final LongBigArray rowIdOutput = new LongBigArray();
    private long currentGroupSize;
    private int currentIndexInGroup;

    ResultIterator() {
      ImmutableList.Builder<TSDataType> sourceTypesBuilders =
          ImmutableList.<TSDataType>builder().addAll(sourceTypes);
      if (produceRowNumber) {
        sourceTypesBuilders.add(TSDataType.INT64);
      }
      pageBuilder = new TsBlockBuilder(sourceTypesBuilders.build());
    }

    @Override
    protected TsBlock computeNext() {
      pageBuilder.reset();
      while (!pageBuilder.isFull()) {
        while (currentIndexInGroup >= currentGroupSize) {
          if (currentGroupId + 1 >= groupIdCount) {
            if (pageBuilder.isEmpty()) {
              return endOfData();
            }
            return pageBuilder.build(
                new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, pageBuilder.getPositionCount()));
          }
          currentGroupId++;
          currentGroupSize = groupedTopNRowNumberAccumulator.drainTo(currentGroupId, rowIdOutput);
          currentIndexInGroup = 0;
        }

        long rowId = rowIdOutput.get(currentIndexInGroup);
        TsBlock page = pageManager.getPage(rowId);
        int position = pageManager.getPosition(rowId);
        for (int i = 0; i < sourceTypes.size(); i++) {
          ColumnBuilder builder = pageBuilder.getColumnBuilder(i);
          Column column = page.getColumn(i);
          builder.write(column, position);
        }
        if (produceRowNumber) {
          ColumnBuilder builder = pageBuilder.getColumnBuilder(sourceTypes.size());
          builder.writeLong(currentGroupId + 1);
        }
        pageBuilder.declarePosition();
        currentIndexInGroup++;

        // Deference the row for hygiene, but no need to compact them at this point
        pageManager.dereference(rowId);
      }

      if (pageBuilder.isEmpty()) {
        return endOfData();
      }
      return pageBuilder.build(
          new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, pageBuilder.getPositionCount()));
    }
  }
}
