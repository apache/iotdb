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

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;
import org.apache.iotdb.db.utils.SetThreadName;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.protocol.session.IClientSession.SqlDialect.TREE;

/**
 * Used to plan a fragment instance. One fragment instance could be split into multiple pipelines so
 * that a fragment instance could be run in parallel, and thus we can take full advantages of
 * multi-cores.
 */
public class LocalExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);
  private static final long ALLOCATE_MEMORY_FOR_OPERATORS;
  private static final long MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;

  public final Metadata metadata = new TableMetadataImpl();

  static {
    IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    ALLOCATE_MEMORY_FOR_OPERATORS = CONFIG.getAllocateMemoryForOperators();
    MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD =
        (long)
            ((ALLOCATE_MEMORY_FOR_OPERATORS) * (1.0 - CONFIG.getMaxAllocateMemoryRatioForLoad()));
  }

  /** allocated memory for operator execution */
  private long freeMemoryForOperators = ALLOCATE_MEMORY_FOR_OPERATORS;

  public long getFreeMemoryForOperators() {
    return freeMemoryForOperators;
  }

  public long getFreeMemoryForLoadTsFile() {
    return freeMemoryForOperators - MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
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

    Operator root = generateOperator(instanceContext, context, plan);

    PipelineMemoryEstimator memoryEstimator =
        context.constructPipelineMemoryEstimator(root, null, plan, -1);
    // set the map to null for gc
    context.invalidateParentPlanNodeIdToMemoryEstimator();

    // check whether current free memory is enough to execute current query
    long estimatedMemorySize = checkMemory(memoryEstimator, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), estimatedMemorySize);

    instanceContext.setSourcePaths(collectSourcePaths(context));
    instanceContext.setDevicePathsToContext(collectDevicePathsToContext(context));
    instanceContext.setQueryDataSourceType(
        getQueryDataSourceType((DataDriverContext) context.getDriverContext()));

    context.getTimePartitions().ifPresent(instanceContext::setTimePartitions);

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      ISchemaRegion schemaRegion)
      throws MemoryNotEnoughException {
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(types, instanceContext, schemaRegion);

    Operator root = generateOperator(instanceContext, context, plan);

    PipelineMemoryEstimator memoryEstimator =
        context.constructPipelineMemoryEstimator(root, null, plan, -1);
    // set the map to null for gc
    context.invalidateParentPlanNodeIdToMemoryEstimator();

    // check whether current free memory is enough to execute current query
    checkMemory(memoryEstimator, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), 0);

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  private Operator generateOperator(
      FragmentInstanceContext instanceContext, LocalExecutionPlanContext context, PlanNode node) {
    // Generate pipelines, return the last pipeline data structure
    // TODO Replace operator with operatorFactory to build multiple driver for one pipeline
    Operator root;
    IClientSession.SqlDialect sqlDialect =
        instanceContext.getSessionInfo() == null
            ? TREE
            : instanceContext.getSessionInfo().getSqlDialect();
    switch (sqlDialect) {
      case TREE:
        instanceContext.setIgnoreAllNullRows(true);
        root = node.accept(new OperatorTreeGenerator(), context);
        break;
      case TABLE:
        instanceContext.setIgnoreAllNullRows(false);
        root = node.accept(new TableOperatorGenerator(metadata), context);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown sql dialect: %s", sqlDialect));
    }
    return root;
  }

  private long checkMemory(
      final PipelineMemoryEstimator memoryEstimator, FragmentInstanceStateMachine stateMachine)
      throws MemoryNotEnoughException {

    // if it is disabled, just return
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableQueryMemoryEstimation()
        && !IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return 0;
    }

    long estimatedMemorySize = memoryEstimator.getEstimatedMemoryUsageInBytes();

    QueryRelatedResourceMetricSet.getInstance().updateEstimatedMemory(estimatedMemorySize);

    synchronized (this) {
      if (estimatedMemorySize > freeMemoryForOperators) {
        throw new MemoryNotEnoughException(
            String.format(
                "There is not enough memory to execute current fragment instance, "
                    + "current remaining free memory is %dB, "
                    + "estimated memory usage for current fragment instance is %dB",
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

  private QueryDataSourceType getQueryDataSourceType(DataDriverContext dataDriverContext) {
    return dataDriverContext.getQueryDataSourceType().orElse(QueryDataSourceType.SERIES_SCAN);
  }

  private Map<IDeviceID, DeviceContext> collectDevicePathsToContext(
      LocalExecutionPlanContext context) {
    DataDriverContext dataDriverContext = (DataDriverContext) context.getDriverContext();
    Map<IDeviceID, DeviceContext> deviceContextMap = dataDriverContext.getDeviceIDToContext();
    dataDriverContext.clearDeviceIDToContext();
    return deviceContextMap;
  }

  private List<IFullPath> collectSourcePaths(LocalExecutionPlanContext context) {
    List<IFullPath> sourcePaths = new ArrayList<>();
    context
        .getPipelineDriverFactories()
        .forEach(
            pipeline -> {
              DataDriverContext dataDriverContext = (DataDriverContext) pipeline.getDriverContext();
              sourcePaths.addAll(dataDriverContext.getPaths());
              dataDriverContext.clearPaths();
            });
    return sourcePaths;
  }

  public synchronized boolean forceAllocateFreeMemoryForOperators(long memoryInBytes) {
    if (freeMemoryForOperators - memoryInBytes <= MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD) {
      return false;
    } else {
      freeMemoryForOperators -= memoryInBytes;
      return true;
    }
  }

  public synchronized long tryAllocateFreeMemoryForOperators(long memoryInBytes) {
    if (freeMemoryForOperators - memoryInBytes <= MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD) {
      long result = freeMemoryForOperators - MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
      freeMemoryForOperators = MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
      return result;
    } else {
      freeMemoryForOperators -= memoryInBytes;
      return memoryInBytes;
    }
  }

  public synchronized void reserveFromFreeMemoryForOperators(
      final long memoryInBytes,
      final long reservedBytes,
      final String queryId,
      final String contextHolder) {
    if (memoryInBytes > freeMemoryForOperators) {
      throw new MemoryNotEnoughException(
          String.format(
              "There is not enough memory for Query %s, the contextHolder is %s,"
                  + "current remaining free memory is %dB, "
                  + "already reserved memory for this context in total is %dB, "
                  + "the memory requested this time is %dB",
              queryId, contextHolder, freeMemoryForOperators, reservedBytes, memoryInBytes));
    } else {
      freeMemoryForOperators -= memoryInBytes;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "[ConsumeMemory] consume: {}, current remaining memory: {}",
            memoryInBytes,
            freeMemoryForOperators);
      }
    }
  }

  public synchronized void releaseToFreeMemoryForOperators(final long memoryInBytes) {
    freeMemoryForOperators += memoryInBytes;
  }

  public long getAllocateMemoryForOperators() {
    return ALLOCATE_MEMORY_FOR_OPERATORS;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final LocalExecutionPlanner INSTANCE = new LocalExecutionPlanner();
  }
}
