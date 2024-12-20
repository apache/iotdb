package org.apache.iotdb.db.queryengine.execution.operator.process.window.function;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate.AggregationWindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate.WindowAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.PartitionExecutor;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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

  // Assume input TsBlock has only one column
  // And only output one column
  public static AggregationWindowFunction createAggregationWindowFunction(
      TAggregationType aggregationType,
      TSDataType inputDataType,
      TSDataType outputDataType,
      boolean ascending
  ) {
    // inputExpressions and inputAttributes are not used in this method
    TableAccumulator accumulator = AccumulatorFactory.createBuiltinAccumulator(aggregationType, Collections.singletonList(inputDataType), new ArrayList<>(), new HashMap<>(), ascending);
    WindowAggregator aggregator = new WindowAggregator(accumulator, outputDataType, Collections.singletonList(0));
    return new AggregationWindowFunction(aggregator);
  }
}
