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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableSet;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.PatternRecognition.rowsPerMatch;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.patternRecognition;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch.ONE;

/**
 * This rule restricts the inputs to PatternRecognitionNode based on which symbols are used by the
 * inner structures of the PatternRecognitionNode. As opposite to PrunePattenRecognitionColumns
 * rule, this rule is not aware of which output symbols of the PatternRecognitionNode are used by
 * the upstream plan. Consequentially, it can only prune the inputs which are not exposed to the
 * output. Such possibility applies only to PatternRecognitionNode with the option `ONE ROW PER
 * MATCH`, where only the partitioning symbols are passed to output.
 *
 * <p>This rule is complementary to PrunePatternRecognitionColumns. It can prune
 * PatternRecognitionNode's inputs in cases when there is no narrowing projection on top of the
 * node.
 */
public class PrunePatternRecognitionSourceColumns implements Rule<PatternRecognitionNode> {
  private static final Pattern<PatternRecognitionNode> PATTERN =
      patternRecognition().with(rowsPerMatch().matching(rowsPerMatch -> rowsPerMatch == ONE));

  @Override
  public Pattern<PatternRecognitionNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
    ImmutableSet.Builder<Symbol> referencedInputs = ImmutableSet.builder();

    referencedInputs.addAll(node.getPartitionBy());
    node.getOrderingScheme()
        .ifPresent(orderingScheme -> referencedInputs.addAll(orderingScheme.getOrderBy()));
    node.getHashSymbol().ifPresent(referencedInputs::add);
    node.getMeasures().values().stream()
        .map(Measure::getExpressionAndValuePointers)
        .map(ExpressionAndValuePointers::getInputSymbols)
        .forEach(referencedInputs::addAll);
    node.getVariableDefinitions().values().stream()
        .map(ExpressionAndValuePointers::getInputSymbols)
        .forEach(referencedInputs::addAll);

    return restrictChildOutputs(context.getIdAllocator(), node, referencedInputs.build())
        .map(Result::ofPlanNode)
        .orElse(Result.empty());
  }
}
