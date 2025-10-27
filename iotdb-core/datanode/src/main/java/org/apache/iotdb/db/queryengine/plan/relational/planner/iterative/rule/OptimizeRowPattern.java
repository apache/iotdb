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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrPatternAlternationOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPatternFlattener;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.patternRecognition;

public class OptimizeRowPattern implements Rule<PatternRecognitionNode> {
  private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition();

  @Override
  public Pattern<PatternRecognitionNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
    IrRowPattern optimizedPattern =
        IrPatternAlternationOptimizer.optimize(IrRowPatternFlattener.optimize(node.getPattern()));

    if (optimizedPattern.equals(node.getPattern())) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        new PatternRecognitionNode(
            node.getPlanNodeId(),
            node.getChild(),
            node.getPartitionBy(),
            node.getOrderingScheme(),
            node.getHashSymbol(),
            node.getMeasures(),
            node.getRowsPerMatch(),
            node.getSkipToLabels(),
            node.getSkipToPosition(),
            optimizedPattern,
            node.getVariableDefinitions()));
  }
}
