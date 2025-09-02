/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
      TsBlock tsBlock, List<TSDataType> dataTypes, WindowFunction windowFunction) {
    return createPartitionExecutor(tsBlock, dataTypes, windowFunction, new ArrayList<>());
  }

  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock,
      List<TSDataType> dataTypes,
      WindowFunction windowFunction,
      List<Integer> sortChannels) {
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.RANGE,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.CURRENT_ROW);
    return createPartitionExecutor(tsBlock, dataTypes, windowFunction, frameInfo, sortChannels);
  }

  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock,
      List<TSDataType> dataTypes,
      WindowFunction windowFunction,
      FrameInfo frameInfo) {
    return createPartitionExecutor(
        tsBlock, dataTypes, windowFunction, frameInfo, new ArrayList<>());
  }

  public static PartitionExecutor createPartitionExecutor(
      TsBlock tsBlock,
      List<TSDataType> dataTypes,
      WindowFunction windowFunction,
      FrameInfo frameInfo,
      List<Integer> sortChannels) {
    List<TsBlock> tsBlocks = Collections.singletonList(tsBlock);
    int startIndex = 0, endIndex = tsBlock.getPositionCount();
    List<WindowFunction> windowFunctions = Collections.singletonList(windowFunction);
    List<FrameInfo> frameInfoList = Collections.singletonList(frameInfo);

    // Output channels are contiguous
    ArrayList<Integer> outputChannels = new ArrayList<>();
    for (int i = 0; i < dataTypes.size(); i++) {
      outputChannels.add(i);
    }

    return new PartitionExecutor(
        tsBlocks,
        dataTypes,
        startIndex,
        endIndex,
        outputChannels,
        windowFunctions,
        frameInfoList,
        sortChannels);
  }

  // Assume input TsBlock has only one column
  // And only output one column
  public static AggregationWindowFunction createAggregationWindowFunction(
      TAggregationType aggregationType,
      TSDataType inputDataType,
      TSDataType outputDataType,
      boolean ascending) {
    // inputExpressions and inputAttributes are not used in this method
    TableAccumulator accumulator =
        AccumulatorFactory.createBuiltinAccumulator(
            aggregationType,
            Collections.singletonList(inputDataType),
            new ArrayList<>(),
            new HashMap<>(),
            ascending,
            false);
    WindowAggregator aggregator =
        new WindowAggregator(accumulator, outputDataType, Collections.singletonList(0));
    return new AggregationWindowFunction(aggregator);
  }
}
