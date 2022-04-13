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

package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class SimplePlanNodeRewriter<C> extends PlanVisitor<PlanNode, C> {
  @Override
  public PlanNode visitPlan(PlanNode node, C context) {
    return defaultRewrite(node, context);
  }

  public PlanNode defaultRewrite(PlanNode node, C context) {
    List<PlanNode> children =
        node.getChildren().stream()
            .map(child -> rewrite(child, context))
            .collect(toImmutableList());

    return node.cloneWithChildren(children);
  }

  public PlanNode rewrite(PlanNode node, C userContext) {
    return node.accept(this, userContext);
  }
}
