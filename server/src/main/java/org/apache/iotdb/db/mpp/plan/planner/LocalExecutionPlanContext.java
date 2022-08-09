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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.execution.exchange.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.mpp.execution.timer.RuleBasedTimeSliceAllocator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LocalExecutionPlanContext {
  private final FragmentInstanceContext instanceContext;
  private final List<PartialPath> paths;
  // deviceId -> sensorId Set
  private final Map<String, Set<String>> allSensorsMap;
  // Used to lock corresponding query resources
  private final List<DataSourceOperator> sourceOperators;
  private ISinkHandle sinkHandle;

  private int nextOperatorId = 0;

  private final TypeProvider typeProvider;

  // left is cached last value in last query
  // right is full path for each cached last value
  private List<Pair<TimeValuePair, Binary>> cachedLastValueAndPathList;
  // timeFilter for last query
  private Filter lastQueryTimeFilter;
  // whether we need to update last cache
  private boolean needUpdateLastCache;

  private final RuleBasedTimeSliceAllocator timeSliceAllocator;

  public LocalExecutionPlanContext(
      TypeProvider typeProvider, FragmentInstanceContext instanceContext) {
    this.typeProvider = typeProvider;
    this.instanceContext = instanceContext;
    this.paths = new ArrayList<>();
    this.allSensorsMap = new HashMap<>();
    this.sourceOperators = new ArrayList<>();
    this.timeSliceAllocator = new RuleBasedTimeSliceAllocator();
  }

  public LocalExecutionPlanContext(FragmentInstanceContext instanceContext) {
    this.instanceContext = instanceContext;
    this.paths = new ArrayList<>();
    this.allSensorsMap = new HashMap<>();
    this.sourceOperators = new ArrayList<>();
    this.typeProvider = null;

    // only used in `order by heat`
    this.timeSliceAllocator = new RuleBasedTimeSliceAllocator();
  }

  public int getNextOperatorId() {
    return nextOperatorId++;
  }

  public List<PartialPath> getPaths() {
    return paths;
  }

  public Set<String> getAllSensors(String deviceId, String sensorId) {
    Set<String> allSensors = allSensorsMap.computeIfAbsent(deviceId, k -> new HashSet<>());
    allSensors.add(sensorId);
    return allSensors;
  }

  public List<DataSourceOperator> getSourceOperators() {
    return sourceOperators;
  }

  public void addPath(PartialPath path) {
    paths.add(path);
  }

  public void addSourceOperator(DataSourceOperator sourceOperator) {
    sourceOperators.add(sourceOperator);
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

  public ISinkHandle getSinkHandle() {
    return sinkHandle;
  }

  public void setSinkHandle(ISinkHandle sinkHandle) {
    requireNonNull(sinkHandle, "sinkHandle is null");
    checkArgument(this.sinkHandle == null, "There must be at most one SinkNode");

    this.sinkHandle = sinkHandle;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public RuleBasedTimeSliceAllocator getTimeSliceAllocator() {
    return timeSliceAllocator;
  }

  public FragmentInstanceContext getInstanceContext() {
    return instanceContext;
  }

  public Filter getLastQueryTimeFilter() {
    return lastQueryTimeFilter;
  }

  public boolean isNeedUpdateLastCache() {
    return needUpdateLastCache;
  }
}
