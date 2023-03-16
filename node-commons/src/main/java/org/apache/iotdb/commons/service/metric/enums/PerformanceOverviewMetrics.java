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

package org.apache.iotdb.commons.service.metric.enums;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.HashMap;
import java.util.Map;

public class PerformanceOverviewMetrics implements IMetricSet {
  private static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();
  private static final String PERFORMANCE_OVERVIEW_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_DETAIL.toString();
  public static final String AUTHORITY = "authority";
  public static final String PARSER = "parser";
  public static final String ANALYZER = "analyzer";
  public static final String PLANNER = "planner";
  public static final String SCHEDULER = "scheduler";

  static {
    metricInfoMap.put(
        AUTHORITY,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), AUTHORITY));
    metricInfoMap.put(
        PARSER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), PARSER));
    metricInfoMap.put(
        ANALYZER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), ANALYZER));
    metricInfoMap.put(
        PLANNER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), PLANNER));
    metricInfoMap.put(
        SCHEDULER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_DETAIL, Tag.STAGE.toString(), SCHEDULER));
  }

  private static final String PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL.toString();
  public static final String LOCAL_SCHEDULE = "local_scheduler";
  public static final String REMOTE_SCHEDULE = "remote_scheduler";

  static {
    metricInfoMap.put(
        LOCAL_SCHEDULE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
            Tag.STAGE.toString(),
            LOCAL_SCHEDULE));
    metricInfoMap.put(
        REMOTE_SCHEDULE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL,
            Tag.STAGE.toString(),
            REMOTE_SCHEDULE));
  }

  private static final String PERFORMANCE_OVERVIEW_LOCAL_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_LOCAL_DETAIL.toString();
  public static final String SCHEMA_VALIDATE = "schema_validate";
  public static final String TRIGGER = "trigger";
  public static final String STORAGE = "storage";

  static {
    metricInfoMap.put(
        SCHEMA_VALIDATE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_LOCAL_DETAIL,
            Tag.STAGE.toString(),
            SCHEMA_VALIDATE));
    metricInfoMap.put(
        TRIGGER,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_LOCAL_DETAIL, Tag.STAGE.toString(), TRIGGER));
    metricInfoMap.put(
        STORAGE,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_LOCAL_DETAIL, Tag.STAGE.toString(), STORAGE));
  }

  private static final String PERFORMANCE_OVERVIEW_STORAGE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_STORAGE_DETAIL.toString();
  public static final String ENGINE = "engine";

  static {
    metricInfoMap.put(
        ENGINE,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_STORAGE_DETAIL, Tag.STAGE.toString(), ENGINE));
  }

  private static final String PERFORMANCE_OVERVIEW_ENGINE_DETAIL =
      Metric.PERFORMANCE_OVERVIEW_ENGINE_DETAIL.toString();
  public static final String LOCK = "lock";
  public static final String MEMORY_BLOCK = "memory_block";
  public static final String CREATE_MEMTABLE_BLOCK = "create_memtable_block";
  public static final String WAL = "wal";
  public static final String MEMTABLE = "memtable";
  public static final String LAST_CACHE = "last_cache";

  static {
    metricInfoMap.put(
        LOCK,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_ENGINE_DETAIL, Tag.STAGE.toString(), LOCK));
    metricInfoMap.put(
        CREATE_MEMTABLE_BLOCK,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            Tag.STAGE.toString(),
            CREATE_MEMTABLE_BLOCK));
    metricInfoMap.put(
        MEMORY_BLOCK,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            Tag.STAGE.toString(),
            MEMORY_BLOCK));
    metricInfoMap.put(
        WAL,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_ENGINE_DETAIL, Tag.STAGE.toString(), WAL));
    metricInfoMap.put(
        MEMTABLE,
        new MetricInfo(
            MetricType.TIMER, PERFORMANCE_OVERVIEW_ENGINE_DETAIL, Tag.STAGE.toString(), MEMTABLE));
    metricInfoMap.put(
        LAST_CACHE,
        new MetricInfo(
            MetricType.TIMER,
            PERFORMANCE_OVERVIEW_ENGINE_DETAIL,
            Tag.STAGE.toString(),
            LAST_CACHE));
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.getOrCreateTimer(
          metricInfo.getName(), MetricLevel.CORE, metricInfo.getTagsInArray());
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.remove(MetricType.TIMER, metricInfo.getName(), metricInfo.getTagsInArray());
    }
  }
}
