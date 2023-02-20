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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

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

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan, TypeProvider types, FragmentInstanceContext instanceContext)
      throws MemoryNotEnoughException, QueryProcessException {
    LocalExecutionPlanContext context = new LocalExecutionPlanContext(types, instanceContext);

    // Generate pipelines, return the last pipeline data structure
    // TODO Replace operator with operatorFactory to build multiple driver for one pipeline
    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    // check whether current free memory is enough to execute current query
    checkMemory(root, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext());

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

    context.addPipelineDriverFactory(root, context.getDriverContext());

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
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
            TSStatusCode.MPP_MEMORY_NOT_ENOUGH.getStatusCode());
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
