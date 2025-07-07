/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.ArrayView;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

/**
 * This class computes an aggregate function result in row pattern recognition context.
 *
 * <p>Expressions in DEFINE and MEASURES clauses can contain aggregate functions. Each of these
 * aggregate functions is transformed into an instance of `MatchAggregation` class.
 *
 * <p>Whenever the aggregate function needs to be evaluated , the method `aggregate()` is called.
 * The returned value is then used to evaluate the enclosing expression.
 *
 * <p>The aggregate function needs to be evaluated in certain cases: 1. during the pattern matching
 * phase, e.g. with a defining condition: `DEFINE A AS avg(B.x) > 0`, the aggregate function `avg`
 * needs to be evaluated over all rows matched so far to label `B` every time the matching algorithm
 * tries to match label `A`. 2. during row pattern measures computation, e.g. with `MEASURES M1 AS
 * RUNNING sum(A.x)`, the running sum must be evaluated over all rows matched to label `A` up to
 * every row included in the match; with `MEASURES M2 AS FINAL sum(A.x)`, the overall sum must be
 * computed for rows matched to label `A` in the entire match, and the result must be propagated for
 * every output row.
 *
 * <p>To avoid duplicate computations, `MatchAggregation` is stateful. The state consists of: - the
 * accumulator, which holds the partial result - the patternAggregator, which determines the new
 * positions to aggregate over since the previous call If the `MatchAggregation` instance is going
 * to be reused for different matches, it has to be `reset` before a new match.
 */
public class PatternAggregator {
  // It stores the relevant information of the aggregation function, including the name of the
  // aggregation function, parameters, and return type.
  private final BoundSignature boundSignature;
  private final TableAccumulator accumulator;

  // one expression corresponds to one instance of `PatternAggregator`, and `argumentChannels`
  // stores all the columns that need to be aggregated in this expression.
  private List<Integer> argumentChannels;
  private PatternAggregationTracker patternAggregationTracker;

  public PatternAggregator(
      BoundSignature boundSignature,
      TableAccumulator accumulator,
      List<Integer> argumentChannels,
      PatternAggregationTracker patternAggregationTracker) {
    this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
    this.accumulator = requireNonNull(accumulator, "accumulato is null");
    this.argumentChannels = argumentChannels;
    this.patternAggregationTracker =
        requireNonNull(patternAggregationTracker, "patternAggregationTracker is null");
    accumulator.reset();
  }

  // reset for a new match during measure computations phase
  public void reset() {
    accumulator.reset();
    patternAggregationTracker.reset();
  }

  /**
   * Identify the new positions for aggregation since the last time this aggregation was run, and
   * add them to `accumulator`. Return the overall aggregation result. This method is used for: -
   * Evaluating labels during pattern matching. In this case, the evaluated label has been appended
   * to `matchedLabels`, - Computing row pattern measures after a non-empty match is found.
   */
  public Object aggregate(
      int currentRow,
      ArrayView matchedLabels,
      Partition partition,
      int partitionStart,
      int patternStart) {

    // calculate the row positions that need to be newly aggregated since the last call (relative to
    // the starting position of the partition)
    ArrayView positions =
        patternAggregationTracker.resolveNewPositions(
            currentRow, matchedLabels, partitionStart, patternStart);

    AggregationMask mask =
        AggregationMask.createSelectedPositions(
            partition.getPositionCount(), positions.toArray(), positions.length());

    // process COUNT()/COUNT(*)
    if (argumentChannels.isEmpty()) { // function with no arguments
      Column[] arguments =
          new Column[] {
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, partition.getPositionCount())
          };
      accumulator.addInput(arguments, mask);
    } else {
      // extract the columns that need to be aggregated.
      int argCount = argumentChannels.size();
      Column[] argumentColumns = new Column[argCount];

      for (int i = 0; i < argCount; i++) {
        int channel = argumentChannels.get(i);
        // Create a `ColumnBuilder` instance using the type of the i-th parameter
        ColumnBuilder builder =
            boundSignature.getArgumentType(i).createColumnBuilder(partition.getPositionCount());

        for (int row = 0; row < partition.getPositionCount(); row++) {
          partition.writeTo(builder, channel, row);
        }

        argumentColumns[i] = builder.build();
      }

      accumulator.addInput(argumentColumns, mask);
    }

    // The return result of the aggregation function is one line
    ColumnBuilder resultBuilder = boundSignature.getReturnType().createColumnBuilder(1);
    accumulator.evaluateFinal(resultBuilder);
    return resultBuilder.build().getObject(0);
  }

  /**
   * Aggregate over empty input. This method is used for computing row pattern measures for empty
   * matches. According to the SQL specification, in such case: - count() aggregation should return
   * 0, - all other aggregations should return null. In Trino, certain aggregations do not follow
   * this pattern (e.g. count_if). This implementation is consistent with aggregations behavior in
   * Trino.
   */
  //  public Block aggregateEmpty() {
  //    if (resultOnEmpty != null) {
  //      return resultOnEmpty;
  //    }
  //    BlockBuilder blockBuilder = boundSignature.getReturnType().createBlockBuilder(null, 1);
  //    accumulatorFactory.get().evaluateFinal(blockBuilder);
  //    resultOnEmpty = blockBuilder.build();
  //    return resultOnEmpty;
  //  }

  public PatternAggregator copy() {
    TableAccumulator accumulatorCopy;
    try {
      accumulatorCopy = accumulator.copy();
    } catch (UnsupportedOperationException e) {
      throw new SemanticException(
          String.format(
              "aggregate function %s does not support copying", boundSignature.getName()));
    }

    return new PatternAggregator(
        boundSignature, accumulatorCopy, argumentChannels, patternAggregationTracker.copy());
  }
}
