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

package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

public class ResultNodeContext {

  private final FragmentInstanceId virtualFragmentInstanceId;
  private final PlanNodeId virtualResultNodeId;

  private TEndPoint upStreamEndpoint;
  private FragmentInstanceId upStreamFragmentInstanceId;
  private PlanNodeId upStreamPlanNodeId;

  public ResultNodeContext(QueryId queryId) {
    this.virtualResultNodeId = queryId.genPlanNodeId();
    this.virtualFragmentInstanceId = queryId.genPlanFragmentId().genFragmentInstanceId();
  }

  public void setUpStream(
      TEndPoint upStreamEndpoint,
      FragmentInstanceId upStreamFragmentInstanceId,
      PlanNodeId upStreamPlanNodeId) {
    this.upStreamEndpoint = upStreamEndpoint;
    this.upStreamFragmentInstanceId = upStreamFragmentInstanceId;
    this.upStreamPlanNodeId = upStreamPlanNodeId;
  }

  public FragmentInstanceId getVirtualFragmentInstanceId() {
    return virtualFragmentInstanceId;
  }

  public PlanNodeId getVirtualResultNodeId() {
    return virtualResultNodeId;
  }

  public TEndPoint getUpStreamEndpoint() {
    return upStreamEndpoint;
  }

  public FragmentInstanceId getUpStreamFragmentInstanceId() {
    return upStreamFragmentInstanceId;
  }

  public PlanNodeId getUpStreamPlanNodeId() {
    return upStreamPlanNodeId;
  }
}
