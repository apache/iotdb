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
package org.apache.iotdb.db.queryengine.plan.planner.plan;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;

import java.util.List;

public class DistributedQueryPlan {
  private MPPQueryContext context;
  private SubPlan rootSubPlan;
  private List<PlanFragment> fragments;
  private List<FragmentInstance> instances;

  public DistributedQueryPlan(
      MPPQueryContext context,
      SubPlan rootSubPlan,
      List<PlanFragment> fragments,
      List<FragmentInstance> instances) {
    this.context = context;
    this.rootSubPlan = rootSubPlan;
    this.fragments = fragments;
    this.instances = instances;
  }

  public List<PlanFragment> getFragments() {
    return fragments;
  }

  public SubPlan getRootSubPlan() {
    return rootSubPlan;
  }

  public MPPQueryContext getContext() {
    return context;
  }

  public List<FragmentInstance> getInstances() {
    return instances;
  }
}
