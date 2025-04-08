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
  public boolean evaluateLabel(ArrayView matchedLabels) {
    int label = matchedLabels.get(matchedLabels.length() - 1);
    // The variable `evaluation` stores the logic for determining whether a row can be identified as
    // `label`
    PatternVariableComputation patternVariableComputation = patternVariableComputations.get(label);
    // Invoke the `test` method of 'evaluation' to determine whether the row can be recognized as
    // `label`
    return patternVariableComputation.test(
        matchedLabels,
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
        List<String> labelNames) {
      super(valueAccessors, computation);
      this.labelNames = requireNonNull(labelNames, "labelNames is null");
    }

    // TODO: 2.12
    public boolean test(
        ArrayView matchedLabels,
        int partitionStart,
        int searchStart,
        int searchEnd,
        int patternStart,
        long matchNumber,
        Partition partition) {

      // 计算当前匹配到哪一行了
      int currentRow = patternStart + matchedLabels.length() - 1;

      // compute 方法用于计算 RPR 中的表达式
      // DEFINE 子句中定义的识别一个 label 就是一个布尔表达式
      // 计算得到的布尔值就是我们需要的结果

      // TODO: Done by 2.17
      //  感觉没必要传入 partition 对象？？
      //  A: 需要的，因为需要访问 partition 中的数据。也就是访问行中的数据
      Object result =
          this.compute(
              currentRow,
              matchedLabels,
              searchStart,
              searchEnd,
              patternStart,
              matchNumber,
              labelNames,
              partition);

      // ADD 2.17: 因为是一个布尔表达式，所以 result 一定是一个布尔值
      return (boolean) result;
    }
  }
}
