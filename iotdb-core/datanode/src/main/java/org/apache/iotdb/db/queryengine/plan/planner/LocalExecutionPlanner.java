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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.utils.SetThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

  public long getFreeMemoryForOperators() {
    return freeMemoryForOperators;
  }

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      DataNodeQueryContext dataNodeQueryContext)
      throws MemoryNotEnoughException {
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(types, instanceContext, dataNodeQueryContext);

    // Generate pipelines, return the last pipeline data structure
    // TODO Replace operator with operatorFactory to build multiple driver for one pipeline
    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    // check whether current free memory is enough to execute current query
    long estimatedMemorySize = checkMemory(root, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), estimatedMemorySize);

    instanceContext.setSourcePaths(collectSourcePaths(context));

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan, FragmentInstanceContext instanceContext, ISchemaRegion schemaRegion)
      throws MemoryNotEnoughException {
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(instanceContext, schemaRegion);

    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    // check whether current free memory is enough to execute current query
    checkMemory(root, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), 0);

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  private long checkMemory(Operator root, FragmentInstanceStateMachine stateMachine)
      throws MemoryNotEnoughException {

    // if it is disabled, just return
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableQueryMemoryEstimation()
        && !IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return 0;
    }

    long estimatedMemorySize = root.calculateMaxPeekMemory();

    synchronized (this) {
      if (estimatedMemorySize > freeMemoryForOperators) {
        throw new MemoryNotEnoughException(
            String.format(
                "There is not enough memory to execute current fragment instance, current remaining free memory is %d, estimated memory usage for current fragment instance is %d",
                freeMemoryForOperators, estimatedMemorySize));
      } else {
        freeMemoryForOperators -= estimatedMemorySize;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "[ConsumeMemory] consume: {}, current remaining memory: {}",
              estimatedMemorySize,
              freeMemoryForOperators);
        }
      }
    }

    stateMachine.addStateChangeListener(
        newState -> {
          if (newState.isDone()) {
            try (SetThreadName fragmentInstanceName =
                new SetThreadName(stateMachine.getFragmentInstanceId().getFullId())) {
              synchronized (this) {
                this.freeMemoryForOperators += estimatedMemorySize;
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "[ReleaseMemory] release: {}, current remaining memory: {}",
                      estimatedMemorySize,
                      freeMemoryForOperators);
                }
              }
            }
          }
        });
    return estimatedMemorySize;
  }

  private List<PartialPath> collectSourcePaths(LocalExecutionPlanContext context) {
    List<PartialPath> sourcePaths = new ArrayList<>();
    context
        .getPipelineDriverFactories()
        .forEach(
            pipeline ->
                sourcePaths.addAll(((DataDriverContext) pipeline.getDriverContext()).getPaths()));
    return sourcePaths;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final LocalExecutionPlanner INSTANCE = new LocalExecutionPlanner();
  }
}
