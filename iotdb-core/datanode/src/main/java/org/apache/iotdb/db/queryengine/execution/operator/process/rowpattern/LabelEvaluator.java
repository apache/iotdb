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

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LabelEvaluator {
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

  private final List<Evaluation> evaluations;

  //  private final ProjectingPagesWindowIndex windowIndex;
  private final Partition partition;

  public LabelEvaluator(
      long matchNumber,
      int patternStart,
      int partitionStart,
      int searchStart,
      int searchEnd,
      List<Evaluation> evaluations,
      //      ProjectingPagesWindowIndex windowIndex
      Partition partition) {
    this.matchNumber = matchNumber;
    this.patternStart = patternStart;
    this.partitionStart = partitionStart;
    this.searchStart = searchStart;
    this.searchEnd = searchEnd;
    this.evaluations = requireNonNull(evaluations, "evaluations is null");
    //    this.windowIndex = requireNonNull(windowIndex, "windowIndex is null");
    this.partition = requireNonNull(partition, "partition is null");
  }

  public int getInputLength() {
    return searchEnd - patternStart;
  }

  public boolean isMatchingAtPartitionStart() {
    return patternStart == partitionStart;
  }

  // evaluate the last label in matchedLabels. It has been tentatively appended to the match
  // TODO:
  //  matchedLabels是一个数组，数组的最后一个元素是label，evaluateLabel方法用来判断这个label是否符合条件
  //  也就是将当前行识别为label符合条件吗
  public boolean evaluateLabel(ArrayView matchedLabels) {
    int label = matchedLabels.get(matchedLabels.length() - 1);
    // evaluation 存储了判断一个行能否识别为 label 的逻辑
    Evaluation evaluation = evaluations.get(label);
    // 调用 evaluation 的 test 方法，判断能否将该行识别为 label
    return evaluation.test(
        matchedLabels,
        //        aggregations,
        partitionStart,
        searchStart,
        searchEnd,
        patternStart,
        matchNumber,
        //        windowIndex
        partition);
  }

  // 一个 Evaluation 对应一个 label 的识别逻辑
  // 是 PatternExpressionComputation 的特殊形式，专门用于计算 DEFINE 子句中的布尔表达式
  public static class Evaluation extends PatternExpressionComputation {
    // 访问 Partition 中的一组数据。一个访问器访问一个数据。
    // 如 B.value，访问当前行。
    // 如 LAST(B.value)，访问上一行。
    // private final List<PhysicalValueAccessor> valueAccessors;

    // 计算逻辑：用于表示多个访问器对应的数据之间应该如何计算。其实就是一个传统的表达式了。
    // 如一共有两个参数，定义的运算可以为 #0 < #1
    // 如一共有三个参数，定义的运算可以为 #0 + #1 + #2
    // private final Computation computation;

    // precomputed `Block`s with null values for every `PhysicalValuePointer` (see
    // MeasureComputation)
    // private final Column[] nulls;

    // mapping from int representation to label name
    private final List<String> labelNames;

    //    private final ConnectorSession session;

    public Evaluation(
        List<PhysicalValueAccessor> valueAccessors,
        Computation computation,
        List<String> labelNames) {
      //      this.valueAccessors = requireNonNull(valueAccessors, "valueAccessors is null");
      // this.nulls = precomputeNulls(valueAccessors);
      //      this.computation = requireNonNull(computation, "labelNames is null");
      super(valueAccessors, computation);
      this.labelNames = requireNonNull(labelNames, "labelNames is null");
    }

    public static TsBlock[] precomputeNulls(List<PhysicalValueAccessor> valueAccessors) {
      TsBlock[] nulls = new TsBlock[valueAccessors.size()];
      for (int i = 0; i < valueAccessors.size(); i++) {
        PhysicalValueAccessor accessor = valueAccessors.get(i);
        if (accessor instanceof PhysicalValuePointer) {
          //          nulls[i] = nativeValueToBlock(((PhysicalValuePointer) accessor).getType(),
          // null);
          // TODO:
          nulls[i] = new TsBlock(1);
        }
      }
      return nulls;
    }

    //    public List<PhysicalValueAccessor> getvalueAccessors() {
    //      return valueAccessors;
    //    }

    // TODO: 2.12
    public boolean test(
        ArrayView matchedLabels,
        //        MatchAggregation[] aggregations,
        int partitionStart,
        int searchStart,
        int searchEnd,
        int patternStart,
        long matchNumber,
        //        ProjectingPagesWindowIndex windowIndex
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

      //      Object result =
      //          this.compute(
      //              position,
      //              labels,
      //              searchStart,
      //              searchEnd,
      //              currentPosition,
      //              matchNumber,
      //              labelNames,
      //              partition);

      // TODO:
      //      Block result =
      //          compute(
      //              currentRow,
      //              matchedLabels,
      //              aggregations,
      //              partitionStart,
      //              searchStart,
      //              searchEnd,
      //              patternStart,
      //              matchNumber,
      //              windowIndex,
      //              projection,
      //              valueAccessors,
      //              nulls,
      //              labelNames,
      //              session);

      //      return BOOLEAN.getBoolean(result, 0);
      //      return false;

      // ADD 2.17: 因为是一个布尔表达式，所以 result 一定是一个布尔值
      return (boolean) result;
    }
  }

  //  public static class EvaluationSupplier {
  //    private final Supplier<PageProjection> projection;
  //    private final List<PhysicalValueAccessor> valueAccessors;
  //    private final List<String> labelNames;
  //    private final ConnectorSession session;
  //
  //    public EvaluationSupplier(
  //        Supplier<PageProjection> projection,
  //        List<PhysicalValueAccessor> valueAccessors,
  //        List<String> labelNames,
  //        ConnectorSession session) {
  //      this.projection = requireNonNull(projection, "projection is null");
  //      this.valueAccessors = requireNonNull(valueAccessors, "valueAccessors is null");
  //      this.labelNames = requireNonNull(labelNames, "labelNames is null");
  //      this.session = requireNonNull(session, "session is null");
  //    }
  //
  //    public Evaluation get() {
  //      return new Evaluation(projection.get(), valueAccessors, labelNames, session);
  //    }
  //  }
}
