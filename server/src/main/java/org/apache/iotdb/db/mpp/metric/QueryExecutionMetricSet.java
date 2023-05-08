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

public class QueryExecutionMetricSet implements IMetricSet {

  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  public static final String WAIT_FOR_DISPATCH = "wait_for_dispatch";
  public static final String DISPATCH_READ = "dispatch_read";

  static {
    metricInfoMap.put(
        WAIT_FOR_DISPATCH,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DISPATCHER.toString(),
            Tag.STAGE.toString(),
            WAIT_FOR_DISPATCH));
    metricInfoMap.put(
        DISPATCH_READ,
        new MetricInfo(
            MetricType.TIMER, Metric.DISPATCHER.toString(), Tag.STAGE.toString(), DISPATCH_READ));
  }

  public static final String LOCAL_EXECUTION_PLANNER = "local_execution_planner";
  public static final String QUERY_RESOURCE_INIT = "query_resource_init";
  public static final String GET_QUERY_RESOURCE_FROM_MEM = "get_query_resource_from_mem";
  public static final String DRIVER_INTERNAL_PROCESS = "driver_internal_process";
  public static final String WAIT_FOR_RESULT = "wait_for_result";

  static {
    metricInfoMap.put(
        LOCAL_EXECUTION_PLANNER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.QUERY_EXECUTION.toString(),
            Tag.STAGE.toString(),
            LOCAL_EXECUTION_PLANNER));
    metricInfoMap.put(
        QUERY_RESOURCE_INIT,
        new MetricInfo(
            MetricType.TIMER,
            Metric.QUERY_EXECUTION.toString(),
            Tag.STAGE.toString(),
            QUERY_RESOURCE_INIT));
    metricInfoMap.put(
        GET_QUERY_RESOURCE_FROM_MEM,
        new MetricInfo(
            MetricType.TIMER,
            Metric.QUERY_EXECUTION.toString(),
            Tag.STAGE.toString(),
            GET_QUERY_RESOURCE_FROM_MEM));
    metricInfoMap.put(
        DRIVER_INTERNAL_PROCESS,
        new MetricInfo(
            MetricType.TIMER,
            Metric.QUERY_EXECUTION.toString(),
            Tag.STAGE.toString(),
            DRIVER_INTERNAL_PROCESS));
    metricInfoMap.put(
        WAIT_FOR_RESULT,
        new MetricInfo(
            MetricType.TIMER,
            Metric.QUERY_EXECUTION.toString(),
            Tag.STAGE.toString(),
            WAIT_FOR_RESULT));
  }

  public static final String AGGREGATION_FROM_RAW_DATA = "aggregation_from_raw_data";
  public static final String AGGREGATION_FROM_STATISTICS = "aggregation_from_statistics";

  static {
    metricInfoMap.put(
        AGGREGATION_FROM_RAW_DATA,
        new MetricInfo(
            MetricType.TIMER, Metric.AGGREGATION.toString(), Tag.FROM.toString(), "raw_data"));
    metricInfoMap.put(
        AGGREGATION_FROM_STATISTICS,
        new MetricInfo(
            MetricType.TIMER, Metric.AGGREGATION.toString(), Tag.FROM.toString(), "statistics"));
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
      metricService.getOrCreateTimer(
          metricInfo.getName(), MetricLevel.IMPORTANT, metricInfo.getTagsInArray());
    }
  }
}
