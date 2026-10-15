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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/** Select shared budgets only for FIs whose fan-in can consume any ready input. */
final class SharedExchangeMemoryEligibility {
  private SharedExchangeMemoryEligibility() {}

  static boolean isEligible(PlanNode root) {
    boolean hasCollect = false;
    Deque<PlanNode> nodes = new ArrayDeque<>();
    nodes.push(root);
    while (!nodes.isEmpty()) {
      PlanNode node = nodes.pop();
      if (node instanceof ExchangeNode
          || node
              instanceof
              org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode) {
        // An upstream plan, if still attached, belongs to a different FI.
        continue;
      }
      // Core relational nodes need not implement the serialization-only PlanNode.getType().
      boolean anyReady =
          node instanceof CollectNode
              || node instanceof UnionNode
              || node
                  instanceof
                  org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.CollectNode;
      hasCollect |= anyReady;
      List<PlanNode> children = node.getChildren();
      // Unknown multi-input operators are not implicitly safe. In particular, sequential sinks
      // and joins can wait for an input while another input holds the entire shared budget.
      if (!anyReady && children.size() > 1) {
        return false;
      }
      nodes.addAll(children);
    }
    return hasCollect;
  }
}
