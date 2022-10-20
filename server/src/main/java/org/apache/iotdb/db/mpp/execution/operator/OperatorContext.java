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
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import io.airlift.units.Duration;

import java.util.Objects;

/**
 * Contains information about {@link Operator} execution.
 *
 * <p>Not thread-safe.
 */
public class OperatorContext {

  private final int operatorId;
  private final PlanNodeId planNodeId;
  private final String operatorType;
  private final FragmentInstanceContext instanceContext;

  private Duration maxRunTime;

  public OperatorContext(
      int operatorId,
      PlanNodeId planNodeId,
      String operatorType,
      FragmentInstanceContext instanceContext) {
    this.operatorId = operatorId;
    this.planNodeId = planNodeId;
    this.operatorType = operatorType;
    this.instanceContext = instanceContext;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorType() {
    return operatorType;
  }

  public FragmentInstanceContext getInstanceContext() {
    return instanceContext;
  }

  public Duration getMaxRunTime() {
    return maxRunTime;
  }

  public void setMaxRunTime(Duration maxRunTime) {
    this.maxRunTime = maxRunTime;
  }

  public SessionInfo getSessionInfo() {
    return instanceContext.getSessionInfo();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OperatorContext that = (OperatorContext) o;
    return operatorId == that.operatorId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(operatorId);
  }
}
