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

package org.apache.iotdb.db.queryengine.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class QueryRelatedResourceMetricSet implements IMetricSet {

  public static QueryRelatedResourceMetricSet getInstance() {
    return QueryRelatedResourceMetricSet.InstanceHolder.INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Coordinator
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final Coordinator coordinator = Coordinator.getInstance();
  private static final String METRIC_COORDINATOR = Metric.COORDINATOR.toString();
  private static final String QUERY_EXECUTION_MAP_SIZE = "query_execution_map_size";

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // FragmentInstanceManager
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static final String FRAGMENT_INSTANCE_TIME = "fragment_instance_time";
  private static final String FRAGMENT_INSTANCE_EXECUTION_TIME = "fragment_instance_execution_time";
  private static final String FRAGMENT_INSTANCE_SIZE = "fragment_instance_size";
  private static final String FRAGMENT_INSTANCE_CONTEXT_SIZE = "fragment_instance_context_size";
  private static final String FRAGMENT_INSTANCE_EXECUTION_SIZE = "fragment_instance_execution_size";
  private static final String FRAGMENT_INSTANCE_DRIVER_SIZE = "fragment_instance_driver_size";

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // FragmentInstanceManager
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final TsFileResourceManager TS_FILE_RESOURCE_MANAGER =
      TsFileResourceManager.getInstance();
  private static final String RESOURCE_INDEX = "resource_index";
  private static final String RESOURCE_INDEX_NUM_TYPE = "number";
  private static final String TOTAL_RESOURCE_NUM = "total_resource_num";
  private static final String DEGRADED_RESOURCE_NUM = "degraded_resource_num";
  private static final String RESOURCE_MEMORY_TYPE = "memory";
  private static final String RESOURCE_INDEX_MAX_MEMORY = "max_memory";
  private static final String RESOURCE_INDEX_USED_MEMORY = "used_memory";

  private Timer fragmentInstanceExecutionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Histogram fragmentInstanceContextSizeHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram fragmentInstanceExecutionSizeHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram fragmentInstanceDriverSizeHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  public void updateFragmentInstanceCount(
      long fragmentInstanceContextSize,
      long fragmentInstanceExecutionSize,
      long fragmentInstanceDriverSize) {
    fragmentInstanceContextSizeHistogram.update(fragmentInstanceContextSize);
    fragmentInstanceExecutionSizeHistogram.update(fragmentInstanceExecutionSize);
    fragmentInstanceDriverSizeHistogram.update(fragmentInstanceDriverSize);
  }

  public void updateFragmentInstanceTime(long cost) {
    fragmentInstanceExecutionTimer.updateMillis(cost);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // MemoryPool
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final MemoryPool memoryPool =
      MPPDataExchangeService.getInstance()
          .getMPPDataExchangeManager()
          .getLocalMemoryManager()
          .getQueryPool();
  private static final String MEMORY_POOL = Metric.MEMORY_POOL.toString();
  private static final String MAX_BYTES = "max_bytes";
  private static final String REMAINING_BYTES = "remaining_bytes";
  private static final String QUERY_MEMORY_RESERVATION_SIZE = "query_memory_reservation_size";
  private static final String MEMORY_RESERVATION_SIZE = "memory_reservation_size";

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // LocalExecutionPlanner
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static final LocalExecutionPlanner localExecutionPlanner =
      LocalExecutionPlanner.getInstance();
  private static final String LOCAL_EXECUTION_PLANNER = Metric.LOCAL_EXECUTION_PLANNER.toString();
  private static final String FREE_MEMORY_FOR_OPERATORS = "free_memory_for_operators";
  private static final String ESTIMATED_MEMORY_SIZE = "estimated_memory_size";
  private Histogram estimatedMemoryHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  public void updateEstimatedMemory(long memory) {
    estimatedMemoryHistogram.update(memory);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // Coordinator
    metricService.createAutoGauge(
        METRIC_COORDINATOR,
        MetricLevel.IMPORTANT,
        coordinator,
        Coordinator::getQueryExecutionMapSize,
        Tag.NAME.toString(),
        QUERY_EXECUTION_MAP_SIZE);

    // FragmentInstanceManager
    fragmentInstanceExecutionTimer =
        metricService.getOrCreateTimer(
            Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            FRAGMENT_INSTANCE_TIME,
            Tag.NAME.toString(),
            FRAGMENT_INSTANCE_EXECUTION_TIME);
    fragmentInstanceContextSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            FRAGMENT_INSTANCE_SIZE,
            Tag.NAME.toString(),
            FRAGMENT_INSTANCE_CONTEXT_SIZE);
    fragmentInstanceExecutionSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            FRAGMENT_INSTANCE_SIZE,
            Tag.NAME.toString(),
            FRAGMENT_INSTANCE_EXECUTION_SIZE);
    fragmentInstanceDriverSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            FRAGMENT_INSTANCE_SIZE,
            Tag.NAME.toString(),
            FRAGMENT_INSTANCE_DRIVER_SIZE);

    // MemoryPool
    metricService
        .getOrCreateGauge(MEMORY_POOL, MetricLevel.IMPORTANT, Tag.NAME.toString(), MAX_BYTES)
        .set(memoryPool.getMaxBytes());
    metricService.createAutoGauge(
        MEMORY_POOL,
        MetricLevel.IMPORTANT,
        memoryPool,
        MemoryPool::getRemainingBytes,
        Tag.NAME.toString(),
        REMAINING_BYTES);
    metricService.createAutoGauge(
        MEMORY_POOL,
        MetricLevel.IMPORTANT,
        memoryPool,
        MemoryPool::getQueryMemoryReservationSize,
        Tag.NAME.toString(),
        QUERY_MEMORY_RESERVATION_SIZE);
    metricService.createAutoGauge(
        MEMORY_POOL,
        MetricLevel.IMPORTANT,
        memoryPool,
        MemoryPool::getMemoryReservationSize,
        Tag.NAME.toString(),
        MEMORY_RESERVATION_SIZE);

    // LocalExecutionPlanner
    metricService.createAutoGauge(
        LOCAL_EXECUTION_PLANNER,
        MetricLevel.IMPORTANT,
        localExecutionPlanner,
        LocalExecutionPlanner::getFreeMemoryForOperators,
        Tag.NAME.toString(),
        FREE_MEMORY_FOR_OPERATORS);
    estimatedMemoryHistogram =
        metricService.getOrCreateHistogram(
            LOCAL_EXECUTION_PLANNER,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ESTIMATED_MEMORY_SIZE);

    // resource index
    metricService.createAutoGauge(
        RESOURCE_INDEX,
        MetricLevel.IMPORTANT,
        TS_FILE_RESOURCE_MANAGER,
        TsFileResourceManager::getDegradedTimeIndexNum,
        Tag.TYPE.toString(),
        RESOURCE_INDEX_NUM_TYPE,
        Tag.NAME.toString(),
        DEGRADED_RESOURCE_NUM);

    metricService.createAutoGauge(
        RESOURCE_INDEX,
        MetricLevel.IMPORTANT,
        TS_FILE_RESOURCE_MANAGER,
        TsFileResourceManager::getPriorityQueueSize,
        Tag.TYPE.toString(),
        RESOURCE_INDEX_NUM_TYPE,
        Tag.NAME.toString(),
        TOTAL_RESOURCE_NUM);

    metricService.createAutoGauge(
        RESOURCE_INDEX,
        MetricLevel.IMPORTANT,
        TS_FILE_RESOURCE_MANAGER,
        TsFileResourceManager::getTotalTimeIndexMemCost,
        Tag.TYPE.toString(),
        RESOURCE_MEMORY_TYPE,
        Tag.NAME.toString(),
        RESOURCE_INDEX_USED_MEMORY);

    metricService.createAutoGauge(
        RESOURCE_INDEX,
        MetricLevel.IMPORTANT,
        TS_FILE_RESOURCE_MANAGER,
        TsFileResourceManager::getTimeIndexMemoryThreshold,
        Tag.TYPE.toString(),
        RESOURCE_MEMORY_TYPE,
        Tag.NAME.toString(),
        RESOURCE_INDEX_MAX_MEMORY);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // Coordinator
    metricService.remove(
        MetricType.AUTO_GAUGE, METRIC_COORDINATOR, Tag.NAME.toString(), QUERY_EXECUTION_MAP_SIZE);

    // FragmentInstanceManager
    metricService.remove(
        MetricType.TIMER,
        Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
        Tag.TYPE.toString(),
        FRAGMENT_INSTANCE_TIME,
        Tag.NAME.toString(),
        FRAGMENT_INSTANCE_EXECUTION_TIME);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
        Tag.TYPE.toString(),
        FRAGMENT_INSTANCE_SIZE,
        Tag.NAME.toString(),
        FRAGMENT_INSTANCE_CONTEXT_SIZE);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
        Tag.TYPE.toString(),
        FRAGMENT_INSTANCE_SIZE,
        Tag.NAME.toString(),
        FRAGMENT_INSTANCE_EXECUTION_SIZE);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.FRAGMENT_INSTANCE_MANAGER.toString(),
        Tag.TYPE.toString(),
        FRAGMENT_INSTANCE_SIZE,
        Tag.NAME.toString(),
        FRAGMENT_INSTANCE_DRIVER_SIZE);

    // MemoryPool
    metricService.remove(MetricType.GAUGE, MEMORY_POOL, Tag.NAME.toString(), MAX_BYTES);
    metricService.remove(MetricType.AUTO_GAUGE, MEMORY_POOL, Tag.NAME.toString(), REMAINING_BYTES);
    metricService.remove(
        MetricType.AUTO_GAUGE, MEMORY_POOL, Tag.NAME.toString(), QUERY_MEMORY_RESERVATION_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE, MEMORY_POOL, Tag.NAME.toString(), MEMORY_RESERVATION_SIZE);

    // LocalExecutionPlanner
    metricService.remove(
        MetricType.AUTO_GAUGE,
        LOCAL_EXECUTION_PLANNER,
        Tag.NAME.toString(),
        FREE_MEMORY_FOR_OPERATORS);
    metricService.remove(
        MetricType.HISTOGRAM, LOCAL_EXECUTION_PLANNER, Tag.NAME.toString(), ESTIMATED_MEMORY_SIZE);

    // resource index
    metricService.remove(
        MetricType.AUTO_GAUGE,
        RESOURCE_INDEX,
        Tag.TYPE.toString(),
        RESOURCE_INDEX_NUM_TYPE,
        Tag.NAME.toString(),
        DEGRADED_RESOURCE_NUM);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        RESOURCE_INDEX,
        Tag.TYPE.toString(),
        RESOURCE_INDEX_NUM_TYPE,
        Tag.NAME.toString(),
        TOTAL_RESOURCE_NUM);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        RESOURCE_INDEX,
        Tag.TYPE.toString(),
        RESOURCE_MEMORY_TYPE,
        Tag.NAME.toString(),
        RESOURCE_INDEX_USED_MEMORY);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        RESOURCE_INDEX,
        Tag.TYPE.toString(),
        RESOURCE_MEMORY_TYPE,
        Tag.NAME.toString(),
        RESOURCE_INDEX_MAX_MEMORY);
  }

  private QueryRelatedResourceMetricSet() {
    // empty constructor
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final QueryRelatedResourceMetricSet INSTANCE =
        new QueryRelatedResourceMetricSet();
  }
}
