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

package org.apache.iotdb.commons.queryengine.plan.planner.plan.node;

/**
 * Base visitor abstraction for {@link PlanNode}.
 *
 * <p><strong>Fragile contract notice:</strong> some concrete {@link PlanNode#accept(IPlanVisitor,
 * Object)} implementations currently downcast the incoming visitor to {@link
 * ICoreQueryPlanVisitor}. Therefore, every in-tree subtype of {@link IPlanVisitor} is expected to
 * also inherit from {@link ICoreQueryPlanVisitor} today, usually via {@code PlanVisitor}.
 *
 * <p>This constraint can be broken intentionally, but only with full awareness of the follow-up
 * work that is required. If a future implementation directly implements {@link IPlanVisitor}
 * without also inheriting from {@link ICoreQueryPlanVisitor}, some plan nodes may throw {@link
 * ClassCastException} at runtime unless their {@code accept(...)} logic is updated accordingly.
 */
public interface IPlanVisitor<R, C> {

  default R process(PlanNode node, C context) {
    return node.accept(this, context);
  }

  R visitPlan(PlanNode node, C context);
}
