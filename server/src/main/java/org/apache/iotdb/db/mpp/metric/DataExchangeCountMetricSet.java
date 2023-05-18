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

public class DataExchangeCountMetricSet implements IMetricSet {
  public static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();

  public static final String CALLER = "caller";
  public static final String SERVER = "server";
  private static final String SEND_NEW_DATA_BLOCK_NUM = "send_new_data_block_num";
  public static final String SEND_NEW_DATA_BLOCK_NUM_CALLER =
      SEND_NEW_DATA_BLOCK_NUM + "_" + CALLER;
  public static final String SEND_NEW_DATA_BLOCK_NUM_SERVER =
      SEND_NEW_DATA_BLOCK_NUM + "_" + SERVER;
  private static final String ON_ACKNOWLEDGE_DATA_BLOCK_NUM = "on_acknowledge_data_block_num";
  public static final String ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER =
      ON_ACKNOWLEDGE_DATA_BLOCK_NUM + "_" + CALLER;
  public static final String ON_ACKNOWLEDGE_DATA_BLOCK_NUM_SERVER =
      ON_ACKNOWLEDGE_DATA_BLOCK_NUM + "_" + SERVER;
  private static final String GET_DATA_BLOCK_NUM = "get_data_block_num";
  public static final String GET_DATA_BLOCK_NUM_CALLER = GET_DATA_BLOCK_NUM + "_" + CALLER;
  public static final String GET_DATA_BLOCK_NUM_SERVER = GET_DATA_BLOCK_NUM + "_" + SERVER;

  static {
    metricInfoMap.put(
        SEND_NEW_DATA_BLOCK_NUM_CALLER,
        new MetricInfo(
            MetricType.HISTOGRAM,
            Metric.DATA_EXCHANGE_COUNT.toString(),
            Tag.NAME.toString(),
            SEND_NEW_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            CALLER));
    metricInfoMap.put(
        SEND_NEW_DATA_BLOCK_NUM_SERVER,
        new MetricInfo(
            MetricType.HISTOGRAM,
            Metric.DATA_EXCHANGE_COUNT.toString(),
            Tag.NAME.toString(),
            SEND_NEW_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            SERVER));
    metricInfoMap.put(
        ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER,
        new MetricInfo(
            MetricType.HISTOGRAM,
            Metric.DATA_EXCHANGE_COUNT.toString(),
            Tag.NAME.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            CALLER));
    metricInfoMap.put(
        ON_ACKNOWLEDGE_DATA_BLOCK_NUM_SERVER,
        new MetricInfo(
            MetricType.HISTOGRAM,
            Metric.DATA_EXCHANGE_COUNT.toString(),
            Tag.NAME.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            SERVER));
    metricInfoMap.put(
        GET_DATA_BLOCK_NUM_CALLER,
        new MetricInfo(
            MetricType.HISTOGRAM,
            Metric.DATA_EXCHANGE_COUNT.toString(),
            Tag.NAME.toString(),
            GET_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            CALLER));
    metricInfoMap.put(
        GET_DATA_BLOCK_NUM_SERVER,
        new MetricInfo(
            MetricType.HISTOGRAM,
            Metric.DATA_EXCHANGE_COUNT.toString(),
            Tag.NAME.toString(),
            GET_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            SERVER));
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.getOrCreateHistogram(
          metricInfo.getName(), MetricLevel.IMPORTANT, metricInfo.getTagsInArray());
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.remove(
          MetricType.HISTOGRAM, Metric.DATA_EXCHANGE_COUNT.toString(), metricInfo.getTagsInArray());
    }
  }
}
