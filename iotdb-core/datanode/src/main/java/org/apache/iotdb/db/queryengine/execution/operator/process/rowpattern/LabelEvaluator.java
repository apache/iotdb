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

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.ArrayView;

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

  public LabelEvaluator(
      long matchNumber,
      int patternStart,
      int partitionStart,
      int searchStart,
      int searchEnd,
      List<Evaluation> evaluations
      //      ProjectingPagesWindowIndex windowIndex
      ) {
    this.matchNumber = matchNumber;
    this.patternStart = patternStart;
    this.partitionStart = partitionStart;
    this.searchStart = searchStart;
    this.searchEnd = searchEnd;
    this.evaluations = requireNonNull(evaluations, "evaluations is null");
    //    this.windowIndex = requireNonNull(windowIndex, "windowIndex is null");
  }

  public int getInputLength() {
    return searchEnd - patternStart;
  }

  public boolean isMatchingAtPartitionStart() {
    return patternStart == partitionStart;
  }

  // evaluate the last label in matchedLabels. It has been tentatively appended to the match
  public boolean evaluateLabel(ArrayView matchedLabels) {
    int label = matchedLabels.get(matchedLabels.length() - 1);
    Evaluation evaluation = evaluations.get(label);
    return evaluation.test(
        matchedLabels,
        //        aggregations,
        partitionStart,
        searchStart,
        searchEnd,
        patternStart,
        matchNumber
        //        windowIndex
        );
  }

  public static class Evaluation {
    // compiled computation of label-defining boolean expression
    //    private final PageProjection projection;

    // value accessors ordered as expected by the compiled projection
    private final List<PhysicalValueAccessor> expectedLayout;

    // precomputed `Block`s with null values for every `PhysicalValuePointer` (see
    // MeasureComputation)
    private final TsBlock[] nulls;

    // mapping from int representation to label name
    private final List<String> labelNames;

    //    private final ConnectorSession session;

    public Evaluation(
        //        PageProjection projection,
        List<PhysicalValueAccessor> expectedLayout, List<String> labelNames
        //        ConnectorSession session
        ) {
      //      this.projection = requireNonNull(projection, "projection is null");
      this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
      this.nulls = precomputeNulls(expectedLayout);
      this.labelNames = requireNonNull(labelNames, "labelNames is null");
      //      this.session = requireNonNull(session, "session is null");
    }

    public static TsBlock[] precomputeNulls(List<PhysicalValueAccessor> expectedLayout) {
      TsBlock[] nulls = new TsBlock[expectedLayout.size()];
      for (int i = 0; i < expectedLayout.size(); i++) {
        PhysicalValueAccessor accessor = expectedLayout.get(i);
        if (accessor instanceof PhysicalValuePointer) {
          //          nulls[i] = nativeValueToBlock(((PhysicalValuePointer) accessor).getType(),
          // null);
          // TODO:
          nulls[i] = new TsBlock(1);
        }
      }
      return nulls;
    }

    public List<PhysicalValueAccessor> getExpectedLayout() {
      return expectedLayout;
    }

    public boolean test(
        ArrayView matchedLabels,
        //        MatchAggregation[] aggregations,
        int partitionStart,
        int searchStart,
        int searchEnd,
        int patternStart,
        long matchNumber
        //        ProjectingPagesWindowIndex windowIndex
        ) {
      int currentRow = patternStart + matchedLabels.length() - 1;

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
      //              expectedLayout,
      //              nulls,
      //              labelNames,
      //              session);

      //      return BOOLEAN.getBoolean(result, 0);
      return false;
    }
  }

  //  public static class EvaluationSupplier {
  //    private final Supplier<PageProjection> projection;
  //    private final List<PhysicalValueAccessor> expectedLayout;
  //    private final List<String> labelNames;
  //    private final ConnectorSession session;
  //
  //    public EvaluationSupplier(
  //        Supplier<PageProjection> projection,
  //        List<PhysicalValueAccessor> expectedLayout,
  //        List<String> labelNames,
  //        ConnectorSession session) {
  //      this.projection = requireNonNull(projection, "projection is null");
  //      this.expectedLayout = requireNonNull(expectedLayout, "expectedLayout is null");
  //      this.labelNames = requireNonNull(labelNames, "labelNames is null");
  //      this.session = requireNonNull(session, "session is null");
  //    }
  //
  //    public Evaluation get() {
  //      return new Evaluation(projection.get(), expectedLayout, labelNames, session);
  //    }
  //  }
}
