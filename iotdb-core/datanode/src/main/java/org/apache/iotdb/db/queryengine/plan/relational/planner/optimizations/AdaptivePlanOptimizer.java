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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/** This optimizer is needed for adaptive optimization in FTE. */
public interface AdaptivePlanOptimizer extends PlanOptimizer {
  @Override
  default PlanNode optimize(PlanNode plan, Context context) {
    return optimizeAndMarkPlanChanges(plan, context).plan();
  }

  /** Optimize the plan and return the changes made to the plan. */
  Result optimizeAndMarkPlanChanges(PlanNode plan, Context context);

  public class Result {
    private final PlanNode plan;
    private final Set<PlanNodeId> changedPlanNodes;

    /**
     * @param plan The optimized plan
     * @param changedPlanNodes The set of PlanNodeIds that were changed during optimization, as well
     *     as the new PlanNodeIds that were added to the optimized plan.
     */
    public Result(PlanNode plan, Set<PlanNodeId> changedPlanNodes) {
      this.plan = requireNonNull(plan, "plan is null");
      this.changedPlanNodes = requireNonNull(changedPlanNodes, "changedPlanNodes is null");
    }

    public PlanNode plan() {
      return plan;
    }

    public Set<PlanNodeId> changedPlanNodes() {
      return changedPlanNodes;
    }
  }
}
