package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class FunctionTestUtils {
  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock, List<TSDataType> dataTypes, WindowFunction windowFunction
  ) {
    return createPartitionExecutor(tsBlock, dataTypes, windowFunction, new ArrayList<>());
  }

  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock, List<TSDataType> dataTypes, WindowFunction windowFunction, List<Integer> sortChannels
  ) {
    FrameInfo frameInfo = new FrameInfo(FrameInfo.FrameType.RANGE, FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING, FrameInfo.FrameBoundType.CURRENT_ROW);
    return createPartitionExecutor(tsBlock, dataTypes, windowFunction, frameInfo, sortChannels);
  }

  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock, List<TSDataType> dataTypes, WindowFunction windowFunction, FrameInfo frameInfo
  ) {
    return createPartitionExecutor(tsBlock, dataTypes, windowFunction, frameInfo, new ArrayList<>());
  }

  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock, List<TSDataType> dataTypes, WindowFunction windowFunction, FrameInfo frameInfo, List<Integer> sortChannels
  ) {
    List<TsBlock> tsBlocks = Collections.singletonList(tsBlock);
    int startIndex = 0, endIndex = tsBlock.getPositionCount();
    List<WindowFunction> windowFunctions = Collections.singletonList(windowFunction);
    List<FrameInfo> frameInfoList = Collections.singletonList(frameInfo);

    return new PartitionExecutor(tsBlocks, dataTypes, startIndex, endIndex, windowFunctions, frameInfoList, sortChannels);
  }

  // Since data type does not matter in rank window functions
  // We only test integers for simplicity
  public static TsBlock createTsBlockForRankFunction(int[] inputs) {
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
}
