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
package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.mpp.execution.driver.DataDriver;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriver;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.timer.ITimeSliceAllocator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import io.airlift.concurrent.SetThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to plan a fragment instance. Currently, we simply change it from PlanNode to executable
 * Operator tree, but in the future, we may split one fragment instance into multiple pipeline to
 * run a fragment instance parallel and take full advantage of multi-cores
 */
public class LocalExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);

  /** allocated memory for operator execution */
  private long freeMemoryForOperators =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForOperators();

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public DataDriver plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      Filter timeFilter,
      DataRegion dataRegion)
      throws MemoryNotEnoughException {
    LocalExecutionPlanContext context = new LocalExecutionPlanContext(types, instanceContext);

    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    // check whether current free memory is enough to execute current query
    checkMemory(root, instanceContext.getStateMachine());

    ITimeSliceAllocator timeSliceAllocator = context.getTimeSliceAllocator();
    instanceContext
        .getOperatorContexts()
        .forEach(
            operatorContext ->
                operatorContext.setMaxRunTime(timeSliceAllocator.getMaxRunTime(operatorContext)));

    DataDriverContext dataDriverContext =
        new DataDriverContext(
            instanceContext,
            context.getPaths(),
            timeFilter,
            dataRegion,
            context.getSourceOperators());
    instanceContext.setDriverContext(dataDriverContext);
    return new DataDriver(root, context.getSinkHandle(), dataDriverContext);
  }

  public SchemaDriver plan(
      PlanNode plan, FragmentInstanceContext instanceContext, ISchemaRegion schemaRegion)
      throws MemoryNotEnoughException {

    SchemaDriverContext schemaDriverContext =
        new SchemaDriverContext(instanceContext, schemaRegion);
    instanceContext.setDriverContext(schemaDriverContext);

    LocalExecutionPlanContext context = new LocalExecutionPlanContext(instanceContext);

    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    // check whether current free memory is enough to execute current query
    checkMemory(root, instanceContext.getStateMachine());

    ITimeSliceAllocator timeSliceAllocator = context.getTimeSliceAllocator();
    instanceContext
        .getOperatorContexts()
        .forEach(
            operatorContext ->
                operatorContext.setMaxRunTime(timeSliceAllocator.getMaxRunTime(operatorContext)));

    return new SchemaDriver(root, context.getSinkHandle(), schemaDriverContext);
  }

  private void checkMemory(Operator root, FragmentInstanceStateMachine stateMachine)
      throws MemoryNotEnoughException {

    // if it is disabled, just return
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableQueryMemoryEstimation()) {
      return;
    }

    long estimatedMemorySize = root.calculateMaxPeekMemory();

    synchronized (this) {
      if (estimatedMemorySize > freeMemoryForOperators) {
        throw new MemoryNotEnoughException(
            String.format(
                "There is not enough memory to execute current fragment instance, current remaining free memory is %d, estimated memory usage for current fragment instance is %d",
                freeMemoryForOperators, estimatedMemorySize),
            TSStatusCode.MEMORY_NOT_ENOUGH.getStatusCode());
      } else {
        freeMemoryForOperators -= estimatedMemorySize;
        LOGGER.info(
            String.format(
                "consume memory: %d, current remaining memory: %d",
                estimatedMemorySize, freeMemoryForOperators));
      }
    }

    stateMachine.addStateChangeListener(
        newState -> {
          if (newState.isDone()) {
            try (SetThreadName fragmentInstanceName =
                new SetThreadName(stateMachine.getFragmentInstanceId().getFullId())) {
              synchronized (this) {
                this.freeMemoryForOperators += estimatedMemorySize;
                LOGGER.info(
                    String.format(
                        "release memory: %d, current remaining memory: %d",
                        estimatedMemorySize, freeMemoryForOperators));
              }
            }
          }
        });
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final LocalExecutionPlanner INSTANCE = new LocalExecutionPlanner();
  }
}
