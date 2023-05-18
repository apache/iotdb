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
  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  public static final String LOCAL = "local";
  public static final String REMOTE = "remote";
  private static final String SOURCE_HANDLE_GET_TSBLOCK = "source_handle_get_tsblock";
  public static final String SOURCE_HANDLE_GET_TSBLOCK_LOCAL =
      SOURCE_HANDLE_GET_TSBLOCK + "_" + LOCAL;
  public static final String SOURCE_HANDLE_GET_TSBLOCK_REMOTE =
      SOURCE_HANDLE_GET_TSBLOCK + "_" + REMOTE;
  private static final String SOURCE_HANDLE_DESERIALIZE_TSBLOCK =
      "source_handle_deserialize_tsblock";
  public static final String SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL =
      SOURCE_HANDLE_DESERIALIZE_TSBLOCK + "_" + LOCAL;
  public static final String SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE =
      SOURCE_HANDLE_DESERIALIZE_TSBLOCK + "_" + REMOTE;
  private static final String SINK_HANDLE_SEND_TSBLOCK = "sink_handle_send_tsblock";
  public static final String SINK_HANDLE_SEND_TSBLOCK_LOCAL =
      SINK_HANDLE_SEND_TSBLOCK + "_" + LOCAL;
  public static final String SINK_HANDLE_SEND_TSBLOCK_REMOTE =
      SINK_HANDLE_SEND_TSBLOCK + "_" + REMOTE;

  static {
    metricInfoMap.put(
        SOURCE_HANDLE_GET_TSBLOCK_LOCAL,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_GET_TSBLOCK,
            Tag.TYPE.toString(),
            LOCAL));
    metricInfoMap.put(
        SOURCE_HANDLE_GET_TSBLOCK_REMOTE,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_GET_TSBLOCK,
            Tag.TYPE.toString(),
            REMOTE));
    metricInfoMap.put(
        SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK,
            Tag.TYPE.toString(),
            LOCAL));
    metricInfoMap.put(
        SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK,
            Tag.TYPE.toString(),
            REMOTE));
    metricInfoMap.put(
        SINK_HANDLE_SEND_TSBLOCK_LOCAL,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SINK_HANDLE_SEND_TSBLOCK,
            Tag.TYPE.toString(),
            LOCAL));
    metricInfoMap.put(
        SINK_HANDLE_SEND_TSBLOCK_REMOTE,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SINK_HANDLE_SEND_TSBLOCK,
            Tag.TYPE.toString(),
            REMOTE));
  }

  public static final String CALLER = "caller";
  public static final String SERVER = "server";
  private static final String SEND_NEW_DATA_BLOCK_EVENT_TASK = "send_new_data_block_event_task";
  public static final String SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER =
      SEND_NEW_DATA_BLOCK_EVENT_TASK + "_" + CALLER;
  public static final String SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER =
      SEND_NEW_DATA_BLOCK_EVENT_TASK + "_" + SERVER;
  private static final String ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK =
      "on_acknowledge_data_block_event_task";
  public static final String ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER =
      ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK + "_" + CALLER;
  public static final String ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER =
      ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK + "_" + SERVER;
  private static final String GET_DATA_BLOCK_TASK = "get_data_block_task";
  public static final String GET_DATA_BLOCK_TASK_CALLER = GET_DATA_BLOCK_TASK + "_" + CALLER;
  public static final String GET_DATA_BLOCK_TASK_SERVER = GET_DATA_BLOCK_TASK + "_" + SERVER;

  static {
    metricInfoMap.put(
        SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SEND_NEW_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            CALLER));
    metricInfoMap.put(
        SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            SEND_NEW_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            SERVER));
    metricInfoMap.put(
        ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            CALLER));
    metricInfoMap.put(
        ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            SERVER));
    metricInfoMap.put(
        GET_DATA_BLOCK_TASK_CALLER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            GET_DATA_BLOCK_TASK,
            Tag.TYPE.toString(),
            CALLER));
    metricInfoMap.put(
        GET_DATA_BLOCK_TASK_SERVER,
        new MetricInfo(
            MetricType.TIMER,
            Metric.DATA_EXCHANGE_COST.toString(),
            Tag.OPERATION.toString(),
            GET_DATA_BLOCK_TASK,
            Tag.TYPE.toString(),
            SERVER));
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
      metricService.remove(
          MetricType.TIMER, Metric.DATA_EXCHANGE_COST.toString(), metricInfo.getTagsInArray());
    }
  }
}
