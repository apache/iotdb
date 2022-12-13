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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.db.mpp.execution.exchange.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

public class DriverContext {

  protected int pipelineId;
  // only used to pass QueryContext
  private final FragmentInstanceContext fragmentInstanceContext;

  private final List<OperatorContext> operatorContexts = new CopyOnWriteArrayList<>();
  private ISinkHandle sinkHandle;

  private final AtomicBoolean finished = new AtomicBoolean();

  public DriverContext(FragmentInstanceContext fragmentInstanceContext) {
    this.fragmentInstanceContext = fragmentInstanceContext;
  }

  public OperatorContext addOperatorContext(
      int operatorId, PlanNodeId planNodeId, String operatorType) {
    checkArgument(operatorId >= 0, "operatorId is negative");

    for (OperatorContext operatorContext : operatorContexts) {
      checkArgument(
          operatorId != operatorContext.getOperatorId(),
          "A context already exists for operatorId %s",
          operatorId);
    }

    OperatorContext operatorContext =
        new OperatorContext(operatorId, planNodeId, operatorType, this);
    operatorContexts.add(operatorContext);
    return operatorContext;
  }

  public DriverContext createSubDriverContext() {
    return null;
  }

  public void setSinkHandle(ISinkHandle sinkHandle) {
    this.sinkHandle = sinkHandle;
  }

  public ISinkHandle getSinkHandle() {
    return sinkHandle;
  }

  public List<OperatorContext> getOperatorContexts() {
    return operatorContexts;
  }

  public int getId() {
    return pipelineId;
  }

  public FragmentInstanceContext getFragmentInstanceContext() {
    return fragmentInstanceContext;
  }

  public void failed(Throwable cause) {
    fragmentInstanceContext.failed(cause);
    finished.set(true);
  }

  public void finished() {
    finished.compareAndSet(false, true);
  }

  public boolean isDone() {
    return finished.get();
  }
}
