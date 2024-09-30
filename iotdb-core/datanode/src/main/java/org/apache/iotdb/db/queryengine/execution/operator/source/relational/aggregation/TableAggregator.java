/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.primitives.Ints;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.aggregation.TreeAggregator.QUERY_EXECUTION_METRICS;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.AGGREGATION_FROM_STATISTICS;

public class TableAggregator {
  private final Accumulator accumulator;
  private final AggregationNode.Step step;
  private final TSDataType outputType;
  private final int[] inputChannels;
  private final OptionalInt maskChannel;

  public TableAggregator(
      Accumulator accumulator,
      AggregationNode.Step step,
      TSDataType outputType,
      List<Integer> inputChannels,
      OptionalInt maskChannel) {
    this.accumulator = requireNonNull(accumulator, "accumulator is null");
    this.step = requireNonNull(step, "step is null");
    this.outputType = requireNonNull(outputType, "intermediateType is null");
    this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
    this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
    checkArgument(
        step.isInputRaw() || inputChannels.size() == 1,
        "expected 1 input channel for intermediate aggregation");
  }

  public TSDataType getType() {
    return outputType;
  }

  public void processBlock(TsBlock block) {
    if (step.isInputRaw()) {
      Column[] arguments = block.getColumns(inputChannels);
      accumulator.addInput(arguments);
    } else {
      accumulator.addIntermediate(block.getColumn(inputChannels[0]));
    }
  }

  public void evaluate(ColumnBuilder columnBuilder) {
    if (step.isOutputPartial()) {
      accumulator.evaluateIntermediate(columnBuilder);
    } else {
      accumulator.evaluateFinal(columnBuilder);
    }
  }

  /** Used for SeriesAggregateScanOperator. */
  public void processStatistics(Statistics[] valueStatistics) {
    long startTime = System.nanoTime();
    try {
      // TODO verify the rightness
      for (int valueIndex : inputChannels) {
        // int valueIndex = inputLocations[0].getValueColumnIndex();
        accumulator.addStatistics(valueStatistics[valueIndex]);
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          AGGREGATION_FROM_STATISTICS, System.nanoTime() - startTime);
    }
  }

  public boolean hasFinalResult() {
    return accumulator.hasFinalResult();
  }

  public void reset() {
    accumulator.reset();
  }

  public long getEstimatedSize() {
    return accumulator.getEstimatedSize();
  }
}
