package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableWindowOperator implements ProcessOperator {
  // Common fields
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;
  private final TsBlockBuilder resultBuilder;

  // Basic information about window operator
  private final List<WindowFunction> windowFunctions;
  private final List<FrameInfo> frameInfoList;

  // Partition
  private final List<Integer> partitionChannels;
  private final RowComparator partitionComparator;
  private final List<TsBlock> cachedTsBlocks;
  private int startIndexInFirstBlock;

  // Sort
  private final List<Integer> sortChannels;

  // Transformation
  private LinkedList<PartitionExecutor> cachedPartitionExecutors;

  public TableWindowOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      List<TSDataType> outputDataTypes,
      List<WindowFunction> windowFunction,
      List<FrameInfo> frameInfoList,
      List<Integer> partitionChannels,
      List<Integer> sortChannels,
      List<SortOrder> sortOrders) {
    // Common part(among all other operators)
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = ImmutableList.copyOf(inputDataTypes);
    this.resultBuilder = new TsBlockBuilder(outputDataTypes);

    // Basic information part
    this.windowFunctions = windowFunction;
    this.frameInfoList = frameInfoList;

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

    // Misc
    this.cachedTsBlocks = new ArrayList<>();
    this.startIndexInFirstBlock = -1;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return this.operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    // Transform is not finished
    if (!cachedPartitionExecutors.isEmpty()) {
      TsBlock tsBlock = transform();
      if (tsBlock != null) {
        return tsBlock;
      }
      // Receive more data when result TsBlock builder is not full
      // In this case, all partition executors are done
    }

    if (inputOperator.hasNextWithTimer()) {
      // This TsBlock is pre-sorted with PARTITION BY and ORDER BY channels
      TsBlock preSortedBlock = inputOperator.next();

      cachedPartitionExecutors = partition(preSortedBlock);
      if (cachedPartitionExecutors.isEmpty()) {
        // No partition found
        // i.e., partition crosses multiple TsBlocks
        return null;
      }

      // May return null if builder is not full
      return transform();
    } else {
      // Return remaining data in result TsBlockBuilder
      if (!resultBuilder.isEmpty()) {
        TsBlock result = resultBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
        resultBuilder.reset();
        return result;
      } else {
        return null;
      }
    }
  }

  private LinkedList<PartitionExecutor> partition(TsBlock tsBlock) {
    LinkedList<PartitionExecutor> partitionExecutors = new LinkedList<>();

    int partitionStartInCurrentBlock = 0;
    int partitionEndInCurrentBlock = partitionStartInCurrentBlock + 1;

    List<Column> partitionColumns = new ArrayList<>();
    for (int channel : partitionChannels) {
      Column partitionColumn = tsBlock.getColumn(channel);
      partitionColumns.add(partitionColumn);
    }

    // Try to find all partitions
    while (partitionEndInCurrentBlock < tsBlock.getPositionCount()) {
      // Try to find one partition
      while (partitionEndInCurrentBlock < tsBlock.getPositionCount()
          && partitionComparator.equalColumns(partitionColumns, partitionStartInCurrentBlock, partitionEndInCurrentBlock)) {
        partitionEndInCurrentBlock++;
      }

      if (partitionEndInCurrentBlock != tsBlock.getPositionCount()) {
        // Find partition
        PartitionExecutor partition;
        if (partitionStartInCurrentBlock != 0 || startIndexInFirstBlock == -1) {
          // Small partition within this TsBlock
          partition =
              new PartitionExecutor(
                  Collections.singletonList(tsBlock),
                  inputDataTypes,
                  partitionStartInCurrentBlock,
                  partitionEndInCurrentBlock,
                  windowFunctions,
                  frameInfoList,
                  sortChannels);
        } else {
          // Large partition crosses multiple TsBlocks
          cachedTsBlocks.add(tsBlock);
          partition = new PartitionExecutor(
              cachedTsBlocks,
              inputDataTypes,
              startIndexInFirstBlock,
              partitionEndInCurrentBlock,
              windowFunctions,
              frameInfoList,
              sortChannels);
          // Clear TsBlock of last partition
          cachedTsBlocks.clear();
        }
        partitionExecutors.addLast(partition);

        partitionStartInCurrentBlock = partitionEndInCurrentBlock;
        partitionEndInCurrentBlock = partitionStartInCurrentBlock + 1;
      } else {
        // Last partition of TsBlock
        // The beginning of next TsBlock may have rows in this partition
        startIndexInFirstBlock = partitionStartInCurrentBlock;
        cachedTsBlocks.add(tsBlock);
      }
    }

    return partitionExecutors;
  }

  private TsBlock transform() {
    while (!cachedPartitionExecutors.isEmpty()) {
      PartitionExecutor partitionExecutor = cachedPartitionExecutors.getFirst();

      while (!resultBuilder.isFull() && partitionExecutor.hasNext()) {
        partitionExecutor.processNextRow(resultBuilder);
      }

      if (!partitionExecutor.hasNext()) {
        cachedPartitionExecutors.removeFirst();
      }

      if (resultBuilder.isFull()) {
        TsBlock result = resultBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
        resultBuilder.reset();
        return result;
      }
    }

    // Reach partition end, but builder is not full yet
    return null;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !cachedPartitionExecutors.isEmpty() || inputOperator.hasNext() || !resultBuilder.isEmpty();
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
