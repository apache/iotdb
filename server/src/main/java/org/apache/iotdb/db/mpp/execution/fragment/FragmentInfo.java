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
package org.apache.iotdb.db.mpp.execution.fragment;

import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;

import java.util.List;

public class FragmentInfo {

  private final PlanFragmentId stageId;
  private final FragmentState state;
  private final PlanFragment plan;

  private final List<FragmentInfo> childrenFragments;

  public FragmentInfo(
      PlanFragmentId stageId,
      FragmentState state,
      PlanFragment plan,
      List<FragmentInfo> childrenFragments) {
    this.stageId = stageId;
    this.state = state;
    this.plan = plan;
    this.childrenFragments = childrenFragments;
  }
}
