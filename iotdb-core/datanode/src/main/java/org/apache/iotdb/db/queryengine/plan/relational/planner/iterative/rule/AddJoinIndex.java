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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.join;

/** <b>Optimization phase:</b> Distributed plan planning. */
public class AddJoinIndex implements Rule<JoinNode> {

  private static final Pattern<JoinNode> PATTERN = join();

  @Override
  public Pattern<JoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(JoinNode node, Captures captures, Context context) {
    node.leftTimeColumnIdx =
        node.getLeftChild().getOutputSymbols().indexOf(node.getCriteria().get(0).getLeft());
    node.rightTimeColumnIdx =
        node.getRightChild().getOutputSymbols().indexOf(node.getCriteria().get(0).getRight());

    node.leftOutputSymbolIdx = new int[node.getLeftOutputSymbols().size()];
    for (int i = 0; i < node.leftOutputSymbolIdx.length; i++) {
      node.leftOutputSymbolIdx[i] =
          node.getLeftChild().getOutputSymbols().indexOf(node.getLeftOutputSymbols().get(i));
    }
    node.rightOutputSymbolIdx = new int[node.getRightOutputSymbols().size()];
    for (int i = 0; i < node.rightOutputSymbolIdx.length; i++) {
      node.rightOutputSymbolIdx[i] =
          node.getRightChild().getOutputSymbols().indexOf(node.getRightOutputSymbols().get(i));
    }
    return Result.empty();
  }
}
