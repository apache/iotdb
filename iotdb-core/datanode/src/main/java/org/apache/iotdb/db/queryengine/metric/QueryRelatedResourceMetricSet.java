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
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.execution.plan.LocalExecutionPlanner;
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

  // MemoryPool
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
  }
}
