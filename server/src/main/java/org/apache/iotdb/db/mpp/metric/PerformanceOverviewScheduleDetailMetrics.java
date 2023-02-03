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
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.HashMap;
import java.util.Map;

public class PerformanceOverviewScheduleDetailMetrics implements IMetricSet {
  private static final String metric = Metric.PERFORMANCE_OVERVIEW_SCHEDULE_DETAIL.toString();
  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  public static final String SCHEMA_VALIDATE = "schema_validate";
  public static final String TRIGGER = "trigger";
  public static final String CONSENSUS = "consensus";
  public static final String LOCK = "lock";
  public static final String MEMORY_BLOCK = "memory_block";
  public static final String WAL = "wal";
  public static final String MEMTABLE = "memtable";
  public static final String LAST_CACHE = "last_cache";

  static {
    metricInfoMap.put(
        SCHEMA_VALIDATE,
        new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), SCHEMA_VALIDATE));
    metricInfoMap.put(
        TRIGGER, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), TRIGGER));
    metricInfoMap.put(
        CONSENSUS, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), CONSENSUS));
    metricInfoMap.put(LOCK, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), LOCK));
    metricInfoMap.put(
        MEMORY_BLOCK, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), MEMORY_BLOCK));
    metricInfoMap.put(WAL, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), WAL));
    metricInfoMap.put(
        MEMTABLE, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), MEMTABLE));
    metricInfoMap.put(
        LAST_CACHE, new MetricInfo(MetricType.TIMER, metric, Tag.STAGE.toString(), LAST_CACHE));
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.getOrCreateTimer(
          metricInfo.getName(), MetricLevel.IMPORTANT, metricInfo.getTagsInArray());
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.remove(MetricType.TIMER, metricInfo.getName(), metricInfo.getTagsInArray());
    }
  }
}
