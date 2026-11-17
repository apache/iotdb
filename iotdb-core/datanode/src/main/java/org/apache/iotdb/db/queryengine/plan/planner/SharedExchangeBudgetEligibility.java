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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Decides whether a fragment may share its exchange budget across INPUT and OUTPUT sides. Each
 * fragment contains at most one multi-child level: either a Collect/Union fan-in, whose ready-first
 * consumption can borrow idle capacity from any child, or an operator that waits for all of its
 * inputs before advancing, which a shared budget could starve. Ordering plays no part in this
 * decision; consumers that require ordered input are handled by the planner.
 */
public final class SharedExchangeBudgetEligibility {
  private SharedExchangeBudgetEligibility() {}

  public static boolean isEligible(PlanNode root) {
    boolean hasCollect = false;
    int multiChildNodeCount = 0;
    Deque<PlanNode> nodes = new ArrayDeque<>();
    nodes.push(root);
    while (!nodes.isEmpty()) {
      PlanNode node = nodes.pop();
      if (node instanceof ExchangeNode) {
        // An upstream plan, if still attached, belongs to a different FI.
        continue;
      }
      // TopK's input ordering assumptions remain unchanged, including its single-child form.
      if (node instanceof TopKNode) {
        return false;
      }
      boolean anyReady = node instanceof CollectNode || node instanceof UnionNode;
      hasCollect |= anyReady;
      List<PlanNode> children = node.getChildren();
      if (children.size() > 1) {
        // A fragment is expected to contain at most one multi-child level. Keep the fallback
        // conservative if a future plan shape violates that invariant.
        if (++multiChildNodeCount > 1 || !anyReady) {
          return false;
        }
      }
      nodes.addAll(children);
    }
    return hasCollect;
  }
}
