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

public class DataExchangeCostMetricSet implements IMetricSet {

  private static final String metric = Metric.DATA_EXCHANGE_COST.toString();

  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  public static final String SOURCE_HANDLE_GET_TSBLOCK_LOCAL = "source_handle_get_tsblock_local";
  public static final String SOURCE_HANDLE_GET_TSBLOCK_REMOTE = "source_handle_get_tsblock_remote";
  public static final String SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL =
      "source_handle_deserialize_tsblock_local";
  public static final String SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE =
      "source_handle_deserialize_tsblock_remote";
  public static final String SINK_HANDLE_SEND_TSBLOCK_LOCAL = "sink_handle_send_tsblock_local";
  public static final String SINK_HANDLE_SEND_TSBLOCK_REMOTE = "sink_handle_send_tsblock_remote";

  static {
    metricInfoMap.put(
        SOURCE_HANDLE_GET_TSBLOCK_LOCAL,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "source_handle_get_tsblock",
            Tag.TYPE.toString(),
            "local"));
    metricInfoMap.put(
        SOURCE_HANDLE_GET_TSBLOCK_REMOTE,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "source_handle_get_tsblock",
            Tag.TYPE.toString(),
            "remote"));
    metricInfoMap.put(
        SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "source_handle_deserialize_tsblock",
            Tag.TYPE.toString(),
            "local"));
    metricInfoMap.put(
        SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "source_handle_deserialize_tsblock",
            Tag.TYPE.toString(),
            "remote"));
    metricInfoMap.put(
        SINK_HANDLE_SEND_TSBLOCK_LOCAL,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "sink_handle_send_tsblock",
            Tag.TYPE.toString(),
            "local"));
    metricInfoMap.put(
        SINK_HANDLE_SEND_TSBLOCK_REMOTE,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "sink_handle_send_tsblock",
            Tag.TYPE.toString(),
            "remote"));
  }

  public static final String SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER =
      "send_new_data_block_event_task_caller";
  public static final String SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER =
      "send_new_data_block_event_task_server";
  public static final String ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER =
      "on_acknowledge_data_block_event_task_caller";
  public static final String ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER =
      "on_acknowledge_data_block_event_task_server";
  public static final String GET_DATA_BLOCK_TASK_CALLER = "get_data_block_task_caller";
  public static final String GET_DATA_BLOCK_TASK_SERVER = "get_data_block_task_server";

  static {
    metricInfoMap.put(
        SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "send_new_data_block_event_task",
            Tag.TYPE.toString(),
            "caller"));
    metricInfoMap.put(
        SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "send_new_data_block_event_task",
            Tag.TYPE.toString(),
            "server"));
    metricInfoMap.put(
        ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "on_acknowledge_data_block_event_task",
            Tag.TYPE.toString(),
            "caller"));
    metricInfoMap.put(
        ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "on_acknowledge_data_block_event_task",
            Tag.TYPE.toString(),
            "server"));
    metricInfoMap.put(
        GET_DATA_BLOCK_TASK_CALLER,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "get_data_block_task",
            Tag.TYPE.toString(),
            "caller"));
    metricInfoMap.put(
        GET_DATA_BLOCK_TASK_SERVER,
        new MetricInfo(
            MetricType.TIMER,
            metric,
            Tag.OPERATION.toString(),
            "get_data_block_task",
            Tag.TYPE.toString(),
            "server"));
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
      metricService.remove(MetricType.TIMER, metric, metricInfo.getTagsInArray());
    }
  }
}
