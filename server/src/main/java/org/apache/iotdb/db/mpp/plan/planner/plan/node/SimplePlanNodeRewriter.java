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

package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class SimplePlanNodeRewriter<C> extends PlanVisitor<List<PlanNode>, C> {
  @Override
  public List<PlanNode> visitPlan(PlanNode node, C context) {
    // TODO: (xingtanzjr) we apply no action for IWritePlanNode currently
    if (node instanceof WritePlanNode) {
      return Collections.singletonList(node);
    }
    return defaultRewrite(node, context);
  }

  public List<PlanNode> defaultRewrite(PlanNode node, C context) {
    List<List<PlanNode>> children =
        node.getChildren().stream()
            .map(child -> rewrite(child, context))
            .collect(toImmutableList());
    PlanNode newNode = node.clone();
    for (List<PlanNode> planNodes : children) {
      planNodes.forEach(newNode::addChild);
    }
    return Collections.singletonList(newNode);
  }

  public List<PlanNode> rewrite(PlanNode node, C userContext) {
    return node.accept(this, userContext);
  }
}
