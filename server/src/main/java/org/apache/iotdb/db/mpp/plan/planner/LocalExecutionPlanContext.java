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
package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISink;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.timer.RuleBasedTimeSliceAllocator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// Attention: We should use thread-safe data structure for members that are shared by all pipelines
public class LocalExecutionPlanContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanContext.class);
  // Save operators in this pipeline, a new one will be created when creating another pipeline
  private final DriverContext driverContext;
  private final AtomicInteger nextOperatorId;
  private final TypeProvider typeProvider;
  private final Map<String, Set<String>> allSensorsMap;
  private int degreeOfParallelism =
      IoTDBDescriptor.getInstance().getConfig().getDegreeOfParallelism();
  // this is shared with all subContexts
  private AtomicInteger nextPipelineId;
  private List<PipelineDriverFactory> pipelineDriverFactories;
  private List<ExchangeOperator> exchangeOperatorList = new ArrayList<>();
  private int exchangeSumNum = 0;

  private final long dataRegionTTL;

  private List<TSDataType> cachedDataTypes;

  // left is cached last value in last query
  // right is full path for each cached last value
  private List<Pair<TimeValuePair, Binary>> cachedLastValueAndPathList;
  // timeFilter for last query
  private Filter lastQueryTimeFilter;
  // whether we need to update last cache
  private boolean needUpdateLastCache;

  // for data region
  public LocalExecutionPlanContext(
      TypeProvider typeProvider, FragmentInstanceContext instanceContext) {
    this.typeProvider = typeProvider;
    this.allSensorsMap = new ConcurrentHashMap<>();
    this.dataRegionTTL = instanceContext.getDataRegion().getDataTTL();
    this.nextOperatorId = new AtomicInteger(0);
    this.nextPipelineId = new AtomicInteger(0);
    this.driverContext = new DataDriverContext(instanceContext, getNextPipelineId());
    this.pipelineDriverFactories = new ArrayList<>();
  }

  // For creating subContext, differ from parent context mainly in driver context
  public LocalExecutionPlanContext(LocalExecutionPlanContext parentContext) {
    this.nextOperatorId = parentContext.nextOperatorId;
    this.typeProvider = parentContext.typeProvider;
    this.allSensorsMap = parentContext.allSensorsMap;
    this.dataRegionTTL = parentContext.dataRegionTTL;
    this.nextPipelineId = parentContext.nextPipelineId;
    this.pipelineDriverFactories = parentContext.pipelineDriverFactories;
    this.degreeOfParallelism = parentContext.degreeOfParallelism;
    this.exchangeSumNum = parentContext.exchangeSumNum;
    this.exchangeOperatorList = parentContext.exchangeOperatorList;
    this.cachedDataTypes = parentContext.cachedDataTypes;
    this.driverContext =
        parentContext.getDriverContext().createSubDriverContext(getNextPipelineId());
  }

  // for schema region
  public LocalExecutionPlanContext(
      FragmentInstanceContext instanceContext, ISchemaRegion schemaRegion) {
    this.allSensorsMap = new ConcurrentHashMap<>();
    this.typeProvider = null;
    this.nextOperatorId = new AtomicInteger(0);
    this.nextPipelineId = new AtomicInteger(0);

    // there is no ttl in schema region, so we don't care this field
    this.dataRegionTTL = Long.MAX_VALUE;
    this.driverContext =
        new SchemaDriverContext(instanceContext, schemaRegion, getNextPipelineId());
    this.pipelineDriverFactories = new ArrayList<>();
  }

  public void addPipelineDriverFactory(Operator operation, DriverContext driverContext) {
    driverContext
        .getOperatorContexts()
        .forEach(
            operatorContext ->
                operatorContext.setMaxRunTime(
                    driverContext.getTimeSliceAllocator().getMaxRunTime(operatorContext)));
    pipelineDriverFactories.add(new PipelineDriverFactory(operation, driverContext));
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

  public Set<String> getAllSensors(String deviceId, String sensorId) {
    Set<String> allSensors = allSensorsMap.computeIfAbsent(deviceId, k -> new HashSet<>());
    allSensors.add(sensorId);
    return allSensors;
  }

  public void setLastQueryTimeFilter(Filter lastQueryTimeFilter) {
    this.lastQueryTimeFilter = lastQueryTimeFilter;
  }

  public void setNeedUpdateLastCache(boolean needUpdateLastCache) {
    this.needUpdateLastCache = needUpdateLastCache;
  }

  public void addCachedLastValue(TimeValuePair timeValuePair, String fullPath) {
    if (cachedLastValueAndPathList == null) {
      cachedLastValueAndPathList = new ArrayList<>();
    }
    cachedLastValueAndPathList.add(new Pair<>(timeValuePair, new Binary(fullPath)));
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

  public RuleBasedTimeSliceAllocator getTimeSliceAllocator() {
    return driverContext.getTimeSliceAllocator();
  }

  public FragmentInstanceContext getInstanceContext() {
    return driverContext.getFragmentInstanceContext();
  }

  public Filter getLastQueryTimeFilter() {
    return lastQueryTimeFilter;
  }

  public boolean isNeedUpdateLastCache() {
    return needUpdateLastCache;
  }

  public long getDataRegionTTL() {
    return dataRegionTTL;
  }
}
