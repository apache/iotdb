package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableWindowOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SortOperator.class);

  // Common fields
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;
  private final TsBlockBuilder tsBlockBuilder;

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
      List<WindowFunction> windowFunctions,
      List<FrameInfo> frameInfoList,
      List<Integer> partitionChannels,
      List<Integer> sortChannels) {
    // Common part(among all other operators)
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = ImmutableList.copyOf(inputDataTypes);
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    // Basic information part
    this.windowFunctions = windowFunctions;
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
    return operatorContext;
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
      if (!tsBlockBuilder.isEmpty()) {
        TsBlock result =
            tsBlockBuilder.build(
                new RunLengthEncodedColumn(
                    TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
        tsBlockBuilder.reset();
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

    // In this stage, we only consider partition channels
    List<Column> partitionColumns = new ArrayList<>();
    for (int channel : partitionChannels) {
      Column partitionColumn = tsBlock.getColumn(channel);
      partitionColumns.add(partitionColumn);
    }

    // Try to find all partitions
    while (partitionEndInCurrentBlock < tsBlock.getPositionCount()) {
      // Try to find one partition
      while (partitionEndInCurrentBlock < tsBlock.getPositionCount()
          && partitionComparator.equalColumns(
              partitionColumns, partitionStartInCurrentBlock, partitionEndInCurrentBlock)) {
        partitionEndInCurrentBlock++;
      }

      if (partitionEndInCurrentBlock != tsBlock.getPositionCount()) {
        // Find partition
        PartitionExecutor partitionExecutor;
        if (partitionStartInCurrentBlock != 0 || startIndexInFirstBlock == -1) {
          // Small partition within this TsBlock
          partitionExecutor =
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
          partitionExecutor =
              new PartitionExecutor(
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
        partitionExecutors.addLast(partitionExecutor);

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

      while (!tsBlockBuilder.isFull() && partitionExecutor.hasNext()) {
        partitionExecutor.processNextRow(tsBlockBuilder);
      }

      if (!partitionExecutor.hasNext()) {
        cachedPartitionExecutors.removeFirst();
      }

      if (tsBlockBuilder.isFull()) {
        TsBlock result =
            tsBlockBuilder.build(
                new RunLengthEncodedColumn(
                    TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
        tsBlockBuilder.reset();
        return result;
      }
    }

    // Reach partition end, but builder is not full yet
    return null;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !cachedPartitionExecutors.isEmpty()
        || inputOperator.hasNext()
        || !tsBlockBuilder.isEmpty();
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemoryFromInput = inputOperator.calculateMaxPeekMemoryWithCounter();
    // PartitionExecutor only hold reference to TsBlock
    // So only cached TsBlocks are considered
    long maxPeekMemoryFromCurrent =
        (long) cachedTsBlocks.size()
                * TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes()
            + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    return Math.max(maxPeekMemoryFromInput, maxPeekMemoryFromCurrent)
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext()
        + (long) cachedTsBlocks.size()
            * TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
