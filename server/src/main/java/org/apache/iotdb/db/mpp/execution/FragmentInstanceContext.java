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
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.query.context.QueryContext;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class FragmentInstanceContext extends QueryContext {

  private FragmentInstanceId id;

  // TODO if we split one fragment instance into multiple pipelines to run, we need to replace it
  // with CopyOnWriteArrayList or some other thread safe data structure
  private final List<OperatorContext> operatorContexts = new ArrayList<>();
  private final long createNanos = System.nanoTime();

  //    private final GcMonitor gcMonitor;
  //    private final AtomicLong startNanos = new AtomicLong();
  //    private final AtomicLong startFullGcCount = new AtomicLong(-1);
  //    private final AtomicLong startFullGcTimeNanos = new AtomicLong(-1);
  //    private final AtomicLong endNanos = new AtomicLong();
  //    private final AtomicLong endFullGcCount = new AtomicLong(-1);
  //    private final AtomicLong endFullGcTimeNanos = new AtomicLong(-1);

  public FragmentInstanceContext(FragmentInstanceId id) {
    this.id = id;
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

  public List<OperatorContext> getOperatorContexts() {
    return operatorContexts;
  }
}
