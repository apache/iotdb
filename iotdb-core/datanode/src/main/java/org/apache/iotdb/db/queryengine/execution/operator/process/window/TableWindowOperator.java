package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableWindowOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TsBlockBuilder tsBlockBuilder;

  private WindowFunction windowFunction;
  private FrameInfo frameInfo;

  private final Comparator<SortKey> comparator;
  private List<SortKey> cachedData;
  private int curRow = -1;

  private int partitionChannel;

  public TableWindowOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      WindowFunction windowFunction,
      FrameInfo frameInfo,
      Comparator<SortKey> comparator,
      int partitionChannel) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.windowFunction = windowFunction;
    this.frameInfo = frameInfo;
    this.comparator = comparator;
    this.partitionChannel = partitionChannel;

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
      TsBlock sorted = buildFinalResult(tsBlockBuilder);
      tsBlockBuilder.reset();

      // Partition
      List<Partition> partitions = partition(sorted);

      TsBlock res = transform(partitions, tsBlockBuilder);
      tsBlockBuilder.reset();
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
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
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
      tsBlockBuilder.declarePosition();
      curRow++;
      if (tsBlockBuilder.isFull()) {
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

    Column partitionColumn = tsBlock.getColumn(partitionChannel);
    while (partitionEnd < partitionColumn.getPositionCount()
        && partitionColumn
            .getObject(partitionEnd)
            .equals(partitionColumn.getObject(partitionStart))) {
      partitionEnd++;

      Partition partition =
          new Partition(tsBlock, partitionStart, partitionEnd, windowFunction, frameInfo);
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

    return builder.build();
  }

  @Override
  public boolean hasNext() throws Exception {
    return this.inputOperator.hasNext();
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
