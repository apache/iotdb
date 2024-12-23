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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimatorFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// Attention: We should use thread-safe data structure for members that are shared by all pipelines
public class LocalExecutionPlanContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanContext.class);
  // Save operators in this pipeline, a new one will be created when creating another pipeline
  private final DriverContext driverContext;
  private final AtomicInteger nextOperatorId;
  private final TypeProvider typeProvider;
  private final Map<IDeviceID, Set<String>> allSensorsMap;
  private int degreeOfParallelism =
      IoTDBDescriptor.getInstance().getConfig().getDegreeOfParallelism();
  // this is shared with all subContexts
  private AtomicInteger nextPipelineId;
  private List<PipelineDriverFactory> pipelineDriverFactories;
  private List<ExchangeOperator> exchangeOperatorList = new ArrayList<>();
  private int exchangeSumNum = 0;

  private List<TSDataType> cachedDataTypes;

  // left is cached last value in last query
  // right is full path for each cached last value
  private List<Pair<TimeValuePair, Binary>> cachedLastValueAndPathList;

  // whether we need to update last cache
  private boolean needUpdateLastCache;
  private boolean needUpdateNullEntry;

  public final DataNodeQueryContext dataNodeQueryContext;

  // null for all time partitions
  // empty for zero time partitions
  // use AtomicReference not for thread-safe, just for updating same field in different pipeline
  private AtomicReference<List<Long>> timePartitions = new AtomicReference<>();

  /** Records the parent of each pipeline. The order of each list does not matter for now. */
  private Map<PlanNodeId, List<PipelineMemoryEstimator>> parentPlanNodeIdToMemoryEstimator =
      new ConcurrentHashMap<>();

  // for data region
  public LocalExecutionPlanContext(
      TypeProvider typeProvider,
      FragmentInstanceContext instanceContext,
      DataNodeQueryContext dataNodeQueryContext) {
    this.typeProvider = typeProvider;
    this.allSensorsMap = new ConcurrentHashMap<>();
    this.nextOperatorId = new AtomicInteger(0);
    this.nextPipelineId = new AtomicInteger(0);
    this.driverContext = new DataDriverContext(instanceContext, getNextPipelineId());
    this.pipelineDriverFactories = new ArrayList<>();
    this.dataNodeQueryContext = dataNodeQueryContext;
  }

  // For creating subContext, differ from parent context mainly in driver context
  public LocalExecutionPlanContext(LocalExecutionPlanContext parentContext) {
    this.nextOperatorId = parentContext.nextOperatorId;
    this.typeProvider = parentContext.typeProvider;
    this.allSensorsMap = parentContext.allSensorsMap;
    this.nextPipelineId = parentContext.nextPipelineId;
    this.pipelineDriverFactories = parentContext.pipelineDriverFactories;
    this.degreeOfParallelism = parentContext.degreeOfParallelism;
    this.exchangeSumNum = parentContext.exchangeSumNum;
    this.exchangeOperatorList = parentContext.exchangeOperatorList;
    this.cachedDataTypes = parentContext.cachedDataTypes;
    this.driverContext =
        parentContext.getDriverContext().createSubDriverContext(getNextPipelineId());
    this.dataNodeQueryContext = parentContext.dataNodeQueryContext;
    this.timePartitions = parentContext.timePartitions;
    this.parentPlanNodeIdToMemoryEstimator = parentContext.parentPlanNodeIdToMemoryEstimator;
  }

  // for schema region
  public LocalExecutionPlanContext(
      final TypeProvider typeProvider,
      final FragmentInstanceContext instanceContext,
      final ISchemaRegion schemaRegion) {
    this.allSensorsMap = new ConcurrentHashMap<>();
    this.typeProvider = typeProvider;
    this.nextOperatorId = new AtomicInteger(0);
    this.nextPipelineId = new AtomicInteger(0);

    this.driverContext =
        new SchemaDriverContext(instanceContext, schemaRegion, getNextPipelineId());
    this.pipelineDriverFactories = new ArrayList<>();
    this.dataNodeQueryContext = null;
  }

  public void addPipelineDriverFactory(
      Operator operation, DriverContext driverContext, long estimatedMemorySize) {
    pipelineDriverFactories.add(
        new PipelineDriverFactory(operation, driverContext, estimatedMemorySize));
  }

  /**
   * Each time we construct a pipeline, we should also construct the memory estimator for the
   * pipeline.
   *
   * @param operation the root operator of the pipeline
   * @param parentPlanNodeId the parent plan node id of the root of the pipeline
   * @param root the root node of the pipeline
   * @param dependencyPipelineIndex the index of the dependency pipeline, -1 if no dependency
   */
  public PipelineMemoryEstimator constructPipelineMemoryEstimator(
      final Operator operation,
      @Nullable final PlanNodeId parentPlanNodeId,
      final PlanNode root,
      final int dependencyPipelineIndex) {
    PipelineMemoryEstimator currentPipelineMemoryEstimator =
        PipelineMemoryEstimatorFactory.createPipelineMemoryEstimator(
            operation, root, dependencyPipelineIndex);
    // As OperatorTreeGenerator traverse the tree in a post-order way, all the children of current
    // pipeline have been recorded in the map.
    List<PipelineMemoryEstimator> childrenMemoryEstimators =
        parentPlanNodeIdToMemoryEstimator.get(root.getPlanNodeId());
    if (childrenMemoryEstimators != null) {
      currentPipelineMemoryEstimator.addChildren(childrenMemoryEstimators);
    }
    if (parentPlanNodeId != null) {
      parentPlanNodeIdToMemoryEstimator
          .computeIfAbsent(parentPlanNodeId, k -> new LinkedList<>())
          .add(currentPipelineMemoryEstimator);
    }
    return currentPipelineMemoryEstimator;
  }

  public LocalExecutionPlanContext createSubContext() {
    return new LocalExecutionPlanContext(this);
  }

  public FragmentInstanceId getFragmentInstanceId() {
    return driverContext.getFragmentInstanceContext().getId();
  }

  public List<PipelineDriverFactory> getPipelineDriverFactories() {
    return pipelineDriverFactories;
  }

  public PipelineDriverFactory getCurrentPipelineDriverFactory() {
    return pipelineDriverFactories.get(pipelineDriverFactories.size() - 1);
  }

  public int getPipelineNumber() {
    return pipelineDriverFactories.size();
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public int getDegreeOfParallelism() {
    return degreeOfParallelism;
  }

  public void setDegreeOfParallelism(int degreeOfParallelism) {
    this.degreeOfParallelism = degreeOfParallelism;
  }

  private int getNextPipelineId() {
    return nextPipelineId.getAndIncrement();
  }

  public boolean isInputDriver() {
    return driverContext.isInputDriver();
  }

  public int getNextOperatorId() {
    return nextOperatorId.getAndIncrement();
  }

  public int getExchangeSumNum() {
    return exchangeSumNum;
  }

  public void setExchangeSumNum(int exchangeSumNum) {
    this.exchangeSumNum = exchangeSumNum;
  }

  public long getMaxBytesOneHandleCanReserve() {
    long maxBytesPerFI = IoTDBDescriptor.getInstance().getConfig().getMaxBytesPerFragmentInstance();
    return exchangeSumNum == 0 ? maxBytesPerFI : maxBytesPerFI / exchangeSumNum;
  }

  public void addExchangeSumNum(int addValue) {
    this.exchangeSumNum += addValue;
  }

  public void addExchangeOperator(ExchangeOperator exchangeOperator) {
    this.exchangeOperatorList.add(exchangeOperator);
  }

  public void setMaxBytesOneHandleCanReserve() {
    long maxBytesOneHandleCanReserve = getMaxBytesOneHandleCanReserve();
    LOGGER.debug(
        "MaxBytesOneHandleCanReserve for ExchangeOperator is {}, exchangeSumNum is {}.",
        maxBytesOneHandleCanReserve,
        exchangeSumNum);
    exchangeOperatorList.forEach(
        exchangeOperator ->
            exchangeOperator.getSourceHandle().setMaxBytesCanReserve(maxBytesOneHandleCanReserve));
  }

  public Set<String> getAllSensors(IDeviceID deviceId, String sensorId) {
    Set<String> allSensors = allSensorsMap.computeIfAbsent(deviceId, k -> new HashSet<>());
    allSensors.add(sensorId);
    return allSensors;
  }

  public void setNeedUpdateLastCache(boolean needUpdateLastCache) {
    this.needUpdateLastCache = needUpdateLastCache;
  }

  public void addCachedLastValue(TimeValuePair timeValuePair, String fullPath) {
    if (cachedLastValueAndPathList == null) {
      cachedLastValueAndPathList = new ArrayList<>();
    }
    cachedLastValueAndPathList.add(
        new Pair<>(timeValuePair, new Binary(fullPath, TSFileConfig.STRING_CHARSET)));
  }

  public List<Pair<TimeValuePair, Binary>> getCachedLastValueAndPathList() {
    return cachedLastValueAndPathList;
  }

  public void setISink(ISink sink) {
    requireNonNull(sink, "sink is null");
    checkArgument(driverContext.getSink() == null, "There must be at most one SinkNode");
    driverContext.setSink(sink);
  }

  public void setCachedDataTypes(List<TSDataType> cachedDataTypes) {
    this.cachedDataTypes = cachedDataTypes;
  }

  public List<TSDataType> getCachedDataTypes() {
    return cachedDataTypes;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public FragmentInstanceContext getInstanceContext() {
    return driverContext.getFragmentInstanceContext();
  }

  public boolean isNeedUpdateLastCache() {
    return needUpdateLastCache;
  }

  public boolean isNeedUpdateNullEntry() {
    return needUpdateNullEntry;
  }

  public void setNeedUpdateNullEntry(boolean needUpdateNullEntry) {
    this.needUpdateNullEntry = needUpdateNullEntry;
  }

  public Filter getGlobalTimeFilter() {
    return driverContext.getFragmentInstanceContext().getGlobalTimeFilter();
  }

  public Optional<List<Long>> getTimePartitions() {
    return Optional.ofNullable(timePartitions.get());
  }

  public void setTimePartitions(List<Long> timePartitions) {
    this.timePartitions.set(timePartitions);
  }

  public ZoneId getZoneId() {
    return driverContext.getFragmentInstanceContext().getSessionInfo().getZoneId();
  }

  public boolean isBuildPlanUseTemplate() {
    return typeProvider.getTemplatedInfo() != null;
  }

  public TemplatedInfo getTemplatedInfo() {
    return typeProvider.getTemplatedInfo();
  }

  public Map<PlanNodeId, List<PipelineMemoryEstimator>> getParentPlanNodeIdToMemoryEstimator() {
    return parentPlanNodeIdToMemoryEstimator;
  }

  public void invalidateParentPlanNodeIdToMemoryEstimator() {
    parentPlanNodeIdToMemoryEstimator = null;
  }
}
