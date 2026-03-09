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

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.Computation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.PatternExpressionComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.ArrayView;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PatternVariableRecognizer {
  private final long matchNumber;

  private final int patternStart;

  // inclusive - the first row of the search partition
  private final int partitionStart;

  // inclusive - the first row of the partition area available for pattern search.
  // this area is the whole partition in case of MATCH_RECOGNIZE, and the area enclosed
  // by the common base frame in case of pattern recognition in WINDOW clause.
  private final int searchStart;

  // exclusive - the first row after the the partition area available for pattern search.
  // this area is the whole partition in case of MATCH_RECOGNIZE, and the area enclosed
  // by the common base frame in case of pattern recognition in WINDOW clause.
  private final int searchEnd;

  private final List<PatternVariableComputation> patternVariableComputations;

  // store all data within the current partition
  private final Partition partition;

  public PatternVariableRecognizer(
      long matchNumber,
      int patternStart,
      int partitionStart,
      int searchStart,
      int searchEnd,
      List<PatternVariableComputation> patternVariableComputations,
      Partition partition) {
    this.matchNumber = matchNumber;
    this.patternStart = patternStart;
    this.partitionStart = partitionStart;
    this.searchStart = searchStart;
    this.searchEnd = searchEnd;
    this.patternVariableComputations =
        requireNonNull(patternVariableComputations, "evaluations is null");
    this.partition = requireNonNull(partition, "partition is null");
  }

  public int getInputLength() {
    return searchEnd - patternStart;
  }

  public boolean isMatchingAtPartitionStart() {
    return patternStart == partitionStart;
  }

  // evaluate the last label in matchedLabels. It has been tentatively appended to the match
  // The `evaluateLabel` method is used to determine whether it is feasible to identify the current
  // row as a label
  public boolean evaluateLabel(ArrayView matchedLabels, PatternAggregator[] patternAggregators) {
    int label = matchedLabels.get(matchedLabels.length() - 1);
    // The variable `evaluation` stores the logic for determining whether a row can be identified as
    // `label`
    PatternVariableComputation patternVariableComputation = patternVariableComputations.get(label);
    // Invoke the `test` method of 'evaluation' to determine whether the row can be recognized as
    // `label`
    return patternVariableComputation.test(
        matchedLabels,
        patternAggregators,
        partitionStart,
        searchStart,
        searchEnd,
        patternStart,
        matchNumber,
        partition);
  }

  /**
   * A `PatternVariableComputation` corresponds to the recognition logic of a pattern variable,
   * specifically designed to evaluate boolean expressions in DEFINE clauses.
   */
  public static class PatternVariableComputation extends PatternExpressionComputation {
    // mapping from int representation to label name
    private final List<String> labelNames;

    public PatternVariableComputation(
        List<PhysicalValueAccessor> valueAccessors,
        Computation computation,
        List<PatternAggregator> patternAggregators,
        List<String> labelNames) {
      super(valueAccessors, computation, patternAggregators);
      this.labelNames = requireNonNull(labelNames, "labelNames is null");
    }

    /**
     * The compute method is used to calculate the expressions in RPR.
     *
     * <p>The identification of a label defined in the DEFINE clause is a Boolean expression and the
     * calculated Boolean value is the result we need.
     */
    public boolean test(
        ArrayView matchedLabels,
        PatternAggregator[] patternAggregators,
        int partitionStart,
        int searchStart,
        int searchEnd,
        int patternStart,
        long matchNumber,
        Partition partition) {
      int currentRow = patternStart + matchedLabels.length() - 1;

      Object result =
          this.compute(
              currentRow,
              matchedLabels,
              patternAggregators,
              partitionStart,
              searchStart,
              searchEnd,
              patternStart,
              matchNumber,
              labelNames,
              partition);

      return (boolean) result;
    }
  }
}
