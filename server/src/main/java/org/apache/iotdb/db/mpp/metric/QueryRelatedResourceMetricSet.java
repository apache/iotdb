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

package org.apache.iotdb.db.mpp.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.db.mpp.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class QueryRelatedResourceMetricSet implements IMetricSet {
  // Coordinator
  private static final Coordinator coordinator = Coordinator.getInstance();
  private static final String COORDINATOR = Metric.COORDINATOR.toString();
  private static final String QUERY_EXECUTION_MAP_SIZE = "query_execution_map_size";

  // FragmentInstanceManager
  private static final FragmentInstanceManager fragmentInstanceManager =
      FragmentInstanceManager.getInstance();
  private static final String FRAGMENT_INSTANCE_MANAGER =
      Metric.FRAGMENT_INSTANCE_MANAGER.toString();
  private static final String INSTANCE_CONTEXT_SIZE = "instance_context_size";
  private static final String INSTANCE_EXECUTION_SIZE = "instance_execution_size";

  // MPPDataExchangeManager
  private static final MPPDataExchangeManager dataExchangeManager =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private static final String DATA_EXCHANGE_MANAGER = Metric.DATA_EXCHANGE_MANAGER.toString();
  private static final String SHUFFLE_SINK_HANDLE_SIZE = "shuffle_sink_handle_size";
  private static final String SOURCE_HANDLE_SIZE = "source_handle_size";

  // MemoryPool
  private static final MemoryPool memoryPool =
      dataExchangeManager.getLocalMemoryManager().getQueryPool();
  private static final String MEMORY_POOL = Metric.MEMORY_POOL.toString();
  private static final String MAX_BYTES = "max_bytes";
  private static final String REMAINING_BYTES = "remaining_bytes";
  private static final String QUERY_MEMORY_RESERVATION_SIZE = "query_memory_reservation_size";
  private static final String MEMORY_RESERVATION_SIZE = "memory_reservation_size";

  // DriverScheduler
  private static final DriverScheduler driverScheduler = DriverScheduler.getInstance();
  private static final String DRIVER_SCHEDULER = Metric.DRIVER_SCHEDULER.toString();
  private static final String READY_QUEUE_SIZE = "ready_queue_size";
  private static final String BLOCKED_TASK_SIZE = "blocked_task_size";
  private static final String TIMEOUT_QUEUE_SIZE = "timeout_queue_size";
  private static final String QUERY_MAP_SIZE = "query_map_size";

  // LocalExecutionPlanner
  private static final LocalExecutionPlanner localExecutionPlanner =
      LocalExecutionPlanner.getInstance();
  private static final String LOCAL_EXECUTION_PLANNER = Metric.LOCAL_EXECUTION_PLANNER.toString();
  private static final String FREE_MEMORY_FOR_OPERATORS = "free_memory_for_operators";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // Coordinator
    metricService.createAutoGauge(
        COORDINATOR,
        MetricLevel.IMPORTANT,
        coordinator,
        Coordinator::getQueryExecutionMapSize,
        Tag.NAME.toString(),
        QUERY_EXECUTION_MAP_SIZE);

    // FragmentInstanceManager
    metricService.createAutoGauge(
        FRAGMENT_INSTANCE_MANAGER,
        MetricLevel.IMPORTANT,
        fragmentInstanceManager,
        FragmentInstanceManager::getInstanceContextSize,
        Tag.NAME.toString(),
        INSTANCE_CONTEXT_SIZE);
    metricService.createAutoGauge(
        FRAGMENT_INSTANCE_MANAGER,
        MetricLevel.IMPORTANT,
        fragmentInstanceManager,
        FragmentInstanceManager::getInstanceExecutionSize,
        Tag.NAME.toString(),
        INSTANCE_EXECUTION_SIZE);

    // MPPDataExchangeManager
    metricService.createAutoGauge(
        DATA_EXCHANGE_MANAGER,
        MetricLevel.IMPORTANT,
        dataExchangeManager,
        MPPDataExchangeManager::getShuffleSinkHandleSize,
        Tag.NAME.toString(),
        SHUFFLE_SINK_HANDLE_SIZE);
    metricService.createAutoGauge(
        DATA_EXCHANGE_MANAGER,
        MetricLevel.IMPORTANT,
        dataExchangeManager,
        MPPDataExchangeManager::getSourceHandleSize,
        Tag.NAME.toString(),
        SOURCE_HANDLE_SIZE);

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

    // DriverScheduler
    metricService.createAutoGauge(
        DRIVER_SCHEDULER,
        MetricLevel.IMPORTANT,
        driverScheduler,
        DriverScheduler::getReadyQueueTaskCount,
        Tag.NAME.toString(),
        READY_QUEUE_SIZE);
    metricService.createAutoGauge(
        DRIVER_SCHEDULER,
        MetricLevel.IMPORTANT,
        driverScheduler,
        DriverScheduler::getBlockQueueTaskCount,
        Tag.NAME.toString(),
        BLOCKED_TASK_SIZE);
    metricService.createAutoGauge(
        DRIVER_SCHEDULER,
        MetricLevel.IMPORTANT,
        driverScheduler,
        DriverScheduler::getTimeoutQueueTaskCount,
        Tag.NAME.toString(),
        TIMEOUT_QUEUE_SIZE);
    metricService.createAutoGauge(
        DRIVER_SCHEDULER,
        MetricLevel.IMPORTANT,
        driverScheduler,
        DriverScheduler::getQueryMapSize,
        Tag.NAME.toString(),
        QUERY_MAP_SIZE);

    // LocalExecutionPlanner
    metricService.createAutoGauge(
        LOCAL_EXECUTION_PLANNER,
        MetricLevel.IMPORTANT,
        localExecutionPlanner,
        LocalExecutionPlanner::getFreeMemoryForOperators,
        Tag.NAME.toString(),
        FREE_MEMORY_FOR_OPERATORS);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // Coordinator
    metricService.remove(
        MetricType.AUTO_GAUGE, COORDINATOR, Tag.NAME.toString(), QUERY_EXECUTION_MAP_SIZE);

    // FragmentInstanceManager
    metricService.remove(
        MetricType.AUTO_GAUGE,
        FRAGMENT_INSTANCE_MANAGER,
        Tag.NAME.toString(),
        INSTANCE_CONTEXT_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        FRAGMENT_INSTANCE_MANAGER,
        Tag.NAME.toString(),
        INSTANCE_EXECUTION_SIZE);

    // MPPDataExchangeManager
    metricService.remove(
        MetricType.AUTO_GAUGE,
        DATA_EXCHANGE_MANAGER,
        Tag.NAME.toString(),
        SHUFFLE_SINK_HANDLE_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE, DATA_EXCHANGE_MANAGER, Tag.NAME.toString(), SOURCE_HANDLE_SIZE);

    // MemoryPool
    metricService.remove(MetricType.GAUGE, MEMORY_POOL, Tag.NAME.toString(), MAX_BYTES);
    metricService.remove(MetricType.AUTO_GAUGE, MEMORY_POOL, Tag.NAME.toString(), REMAINING_BYTES);
    metricService.remove(
        MetricType.AUTO_GAUGE, MEMORY_POOL, Tag.NAME.toString(), QUERY_MEMORY_RESERVATION_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE, MEMORY_POOL, Tag.NAME.toString(), MEMORY_RESERVATION_SIZE);

    // DriverScheduler
    metricService.remove(
        MetricType.AUTO_GAUGE, DRIVER_SCHEDULER, Tag.NAME.toString(), READY_QUEUE_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE, DRIVER_SCHEDULER, Tag.NAME.toString(), BLOCKED_TASK_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE, DRIVER_SCHEDULER, Tag.NAME.toString(), TIMEOUT_QUEUE_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE, DRIVER_SCHEDULER, Tag.NAME.toString(), QUERY_MAP_SIZE);

    // LocalExecutionPlanner
    metricService.remove(
        MetricType.AUTO_GAUGE,
        LOCAL_EXECUTION_PLANNER,
        Tag.NAME.toString(),
        FREE_MEMORY_FOR_OPERATORS);
  }
}
