package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableWindowOperator implements ProcessOperator {
  // Common fields
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;
  private final TsBlockBuilder sortedTsBlockBuilder;
  private final TsBlockBuilder transformTsBlockBuilder;

  // Basic information about window operator
  private WindowFunction windowFunction;
  private FrameInfo frameInfo;

  // Partition
  private List<Integer> partitionChannels;
  private RowComparator partitionComparator;

  // Sort
  private List<Integer> sortChannels;
  private final Comparator<SortKey> comparator;
  // Auxiliary field for sort
  private List<SortKey> cachedData;
  private int curRow = -1;

  public TableWindowOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      WindowFunction windowFunction,
      FrameInfo frameInfo,
      List<Integer> partitionChannels,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders) {
    // Common part(among all other operators)
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = ImmutableList.copyOf(inputDataTypes);
    this.sortedTsBlockBuilder = new TsBlockBuilder(inputDataTypes);
    this.transformTsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    // Basic information part
    this.windowFunction = windowFunction;
    this.frameInfo = frameInfo;

    // TODO: preGroup & preSort
    // Partition Part
    this.partitionChannels = ImmutableList.copyOf(partitionChannels);
    // Acquire partition channels' data types
    List<TSDataType> partitionDataTypes = new ArrayList<>();
    for (Integer channel : partitionChannels) {
      partitionDataTypes.add(inputDataTypes.get(channel));
    }
    this.partitionComparator = new RowComparator(partitionDataTypes);

    // Ordering part
    this.sortChannels = ImmutableList.copyOf(sortChannels);
    // Concat partition and sort channels
    List<Integer> allSortChannels = new ArrayList<>();
    allSortChannels.addAll(partitionChannels);
    allSortChannels.addAll(sortChannels);
    // Construct orderings
    // Partition channels are ascent by default
    List<SortOrder> allSortOrders = new ArrayList<>();
    List<SortOrder> partitionSortOrder =
        Collections.nCopies(partitionChannels.size(), SortOrder.ASC_NULLS_FIRST);
    allSortOrders.addAll(partitionSortOrder);
    allSortOrders.addAll(sortOrders);
    // Acquire these channels data types
    List<TSDataType> allSortDataTypes = new ArrayList<>(partitionDataTypes);
    for (Integer channel : sortChannels) {
      TSDataType dataType = inputDataTypes.get(channel);
      allSortDataTypes.add(dataType);
    }
    // Create comparator for sort
    this.comparator = getComparatorForTable(allSortOrders, allSortChannels, allSortDataTypes);

    this.cachedData = new ArrayList<>();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!inputOperator.hasNextWithTimer()) {
      // Sort
      buildResult();
      TsBlock sorted = buildFinalResult(sortedTsBlockBuilder);
      sortedTsBlockBuilder.reset();

      // Partition
      List<Partition> partitions = partition(sorted);

      TsBlock res = transform(partitions, transformTsBlockBuilder);
      transformTsBlockBuilder.reset();
      return res;
    }

    TsBlock tsBlock = inputOperator.nextWithTimer();
    if (tsBlock == null) {
      return null;
    }

    cacheTsBlock(tsBlock);

    return null;
  }

  private void cacheTsBlock(TsBlock tsBlock) {
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      cachedData.add(new MergeSortKey(tsBlock, i));
    }
  }

  protected void buildResult() throws IoTDBException {
    if (curRow == -1) {
      cachedData.sort(comparator);
      curRow = 0;
    }
    buildTsBlockInMemory();
  }

  private void buildTsBlockInMemory() {
    ColumnBuilder[] valueColumnBuilders = sortedTsBlockBuilder.getValueColumnBuilders();
    for (int i = curRow; i < cachedData.size(); i++) {
      SortKey sortKey = cachedData.get(i);
      TsBlock tsBlock = sortKey.tsBlock;
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        if (tsBlock.getColumn(j).isNull(sortKey.rowIndex)) {
          valueColumnBuilders[j].appendNull();
          continue;
        }
        valueColumnBuilders[j].write(tsBlock.getColumn(j), sortKey.rowIndex);
      }
      sortedTsBlockBuilder.declarePosition();
      curRow++;
      if (sortedTsBlockBuilder.isFull()) {
        break;
      }
    }
  }

  private TsBlock buildFinalResult(TsBlockBuilder resultBuilder) {
    return resultBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
  }

  private List<Partition> partition(TsBlock tsBlock) {
    List<Partition> partitions = new ArrayList<>();

    int partitionStart = 0;
    int partitionEnd = partitionStart + 1;

    List<Column> partitionColumns = new ArrayList<>();
    for (Integer channel : partitionChannels) {
      Column partitionColumn = tsBlock.getColumn(channel);
      partitionColumns.add(partitionColumn);
    }

    while (partitionEnd < tsBlock.getPositionCount()) {
      while (partitionEnd < tsBlock.getPositionCount()
          && partitionComparator.equal(partitionColumns, partitionStart, partitionEnd)) {
        partitionEnd++;
      }

      Partition partition =
          new Partition(tsBlock, inputDataTypes, partitionStart, partitionEnd, windowFunction, frameInfo, sortChannels);
      partitions.add(partition);

      partitionStart = partitionEnd;
      partitionEnd = partitionStart + 1;
    }

    return partitions;
  }

  private TsBlock transform(List<Partition> partitions, TsBlockBuilder builder) {
    for (Partition partition : partitions) {
      while (!builder.isFull() && partition.hasNext()) {
        partition.processNextRow(builder);
      }
    }

    return builder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
  }

  @Override
  public boolean hasNext() throws Exception {
    return this.inputOperator.hasNext()
        || ((curRow == -1 && !cachedData.isEmpty())
            || (curRow != -1 && curRow != cachedData.size()));
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean isFinished() throws Exception {
    return false;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
