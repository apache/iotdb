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

package org.apache.iotdb.db.mpp.plan.planner.distribution;

import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.List;

public class WriteFragmentParallelPlanner implements IFragmentParallelPlaner {

  private SubPlan subPlan;
  private Analysis analysis;
  private MPPQueryContext queryContext;

  public WriteFragmentParallelPlanner(
      SubPlan subPlan, Analysis analysis, MPPQueryContext queryContext) {
    this.subPlan = subPlan;
    this.analysis = analysis;
    this.queryContext = queryContext;
  }

  @Override
  public List<FragmentInstance> parallelPlan() {
    PlanFragment fragment = subPlan.getPlanFragment();
    Filter timeFilter = analysis.getGlobalTimeFilter();
    PlanNode node = fragment.getPlanNodeTree();
    if (!(node instanceof WritePlanNode)) {
      throw new IllegalArgumentException("PlanNode should be IWritePlanNode in WRITE operation");
    }
    List<WritePlanNode> splits = ((WritePlanNode) node).splitByPartition(analysis);
    List<FragmentInstance> ret = new ArrayList<>();
    for (WritePlanNode split : splits) {
      FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragment.getId(), split),
              fragment.getId().genFragmentInstanceId(),
              timeFilter,
              queryContext.getQueryType(),
              queryContext.getTimeOut(),
              queryContext.getSession());
      if (split.getRegionReplicaSet() != null) {
        instance.setExecutorAndHost(new StorageExecutor(split.getRegionReplicaSet()));
      }
      ret.add(instance);
    }
    return ret;
  }
}
