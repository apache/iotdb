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

package org.apache.iotdb.db.mpp.plan.planner.plan;

import java.util.ArrayList;
import java.util.List;

public class SubPlan {
  private PlanFragment planFragment;
  private List<SubPlan> children;

  public SubPlan(PlanFragment planFragment) {
    this.planFragment = planFragment;
    this.children = new ArrayList<>();
  }

  public void setChildren(List<SubPlan> children) {
    this.children = children;
  }

  public void addChild(SubPlan subPlan) {
    this.children.add(subPlan);
  }

  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(
        String.format(
            "SubPlan-%s. RootNodeId: %s%n",
            planFragment.getId(), planFragment.getPlanNodeTree().getPlanNodeId()));
    children.forEach(result::append);
    return result.toString();
  }

  public PlanFragment getPlanFragment() {
    return this.planFragment;
  }

  public List<SubPlan> getChildren() {
    return this.children;
  }

  public List<PlanFragment> getPlanFragmentList() {
    List<PlanFragment> result = new ArrayList<>();
    result.add(this.planFragment);
    this.children.forEach(
        child -> {
          result.addAll(child.getPlanFragmentList());
        });
    return result;
  }
}
