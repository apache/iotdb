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
 * Used to plan a fragment instance. One fragment instance could be split into multiple pipelines so
 * that a fragment instance could be run in parallel, and thus we can take full advantages of
 * multi-cores.
 */
public class LocalExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);

  private static final long QUERY_THREAD_COUNT =
      IoTDBDescriptor.getInstance().getConfig().getQueryThreadCount();

  private static final long ESTIMATED_FI_NUM = 8;

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

    context.addPipelineDriverFactory(
        root, context.getDriverContext(), root.calculateMaxPeekMemory());

    // check whether current free memory is enough to execute current query
    checkMemory(context.getPipelineDriverFactories(), instanceContext.getStateMachine());

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

    context.addPipelineDriverFactory(
        root, context.getDriverContext(), root.calculateMaxPeekMemory());

    // check whether current free memory is enough to execute current query
    checkMemory(context.getPipelineDriverFactories(), instanceContext.getStateMachine());

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  private void checkMemory(
      List<PipelineDriverFactory> pipelineDriverFactories,
      FragmentInstanceStateMachine stateMachine)
      throws MemoryNotEnoughException {

    // if it is disabled, just return
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableQueryMemoryEstimation()
        && !IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return;
    }

    long estimatedMemorySize = calculateEstimatedMemorySize(pipelineDriverFactories);

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
  }

  /**
   * Calculate the estimated memory size this FI would use. Given that there are
   * QUERY_THREAD_COUNT(X) threads, each thread could deal with one DriverTask. Now we have
   * pipelineDriverFactories.size()(Y) DriverTasks(We use pipelineDriverFactories.size() instead of
   * degreeOfParallelism to estimate). Suppose that there are M FragmentInstances at the same time.
   * Then one FragmentInstance could have N Drivers running.
   *
   * <p>N = (X / ( M * Y)) * Y = X / M
   *
   * <p>The estimated running memory size this FI would use is:
   *
   * <p>N * avgMemoryUsedPerDriver = N * totalSizeOfDriver / driverNum
   *
   * <p>Some operators still retain memory when they are not running, so we need to add the size of
   * retained memory of all the Drivers when they are not running.
   *
   * <p>The total estimated memory size this FI would use is:
   *
   * <p>retainedSize + N * totalSizeOfDriver / driverNum
   */
  private long calculateEstimatedMemorySize(
      final List<PipelineDriverFactory> pipelineDriverFactories) {
    long retainedSize =
        pipelineDriverFactories.stream()
            .map(
                pipelineDriverFactory ->
                    pipelineDriverFactory.getOperation().calculateRetainedSizeAfterCallingNext())
            .reduce(0L, Long::sum);
    long totalSizeOfDrivers =
        pipelineDriverFactories.stream()
            .map(PipelineDriverFactory::getEstimatedMemorySize)
            .reduce(0L, Long::sum);
    long runningMemorySize =
        Math.max((QUERY_THREAD_COUNT / ESTIMATED_FI_NUM), 1)
            * (totalSizeOfDrivers / pipelineDriverFactories.size());
    return retainedSize + runningMemorySize;
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
