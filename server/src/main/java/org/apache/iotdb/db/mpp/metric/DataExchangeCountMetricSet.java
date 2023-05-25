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
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class DataExchangeCountMetricSet implements IMetricSet {
  private static final DataExchangeCountMetricSet INSTANCE = new DataExchangeCountMetricSet();

  private DataExchangeCountMetricSet() {
    // empty constructor
  }

  // region data block
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

  private static final MPPDataExchangeManager dataExchangeManager =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private static final String SHUFFLE_SINK_HANDLE_SIZE = "shuffle_sink_handle_size";
  private static final String SOURCE_HANDLE_SIZE = "source_handle_size";

  private Histogram sendNewDataBlockNumCallerHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram sendNewDataBlockNumServerHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram onAcknowledgeDataBlockNumCallerHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram onAcknowledgeDataBlockNumServerHistogram =
      DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram getDataBlockNumCallerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram getDataBlockNumServerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    sendNewDataBlockNumCallerHistogram =
        metricService.getOrCreateHistogram(
            Metric.DATA_EXCHANGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SEND_NEW_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            CALLER);
    sendNewDataBlockNumServerHistogram =
        metricService.getOrCreateHistogram(
            Metric.DATA_EXCHANGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            SEND_NEW_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            SERVER);
    onAcknowledgeDataBlockNumCallerHistogram =
        metricService.getOrCreateHistogram(
            Metric.DATA_EXCHANGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            CALLER);
    onAcknowledgeDataBlockNumServerHistogram =
        metricService.getOrCreateHistogram(
            Metric.DATA_EXCHANGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            SERVER);
    getDataBlockNumCallerHistogram =
        metricService.getOrCreateHistogram(
            Metric.DATA_EXCHANGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            GET_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            CALLER);
    getDataBlockNumServerHistogram =
        metricService.getOrCreateHistogram(
            Metric.DATA_EXCHANGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            GET_DATA_BLOCK_NUM,
            Tag.TYPE.toString(),
            SERVER);
    metricService.createAutoGauge(
        Metric.DATA_EXCHANGE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        dataExchangeManager,
        MPPDataExchangeManager::getShuffleSinkHandleSize,
        Tag.NAME.toString(),
        SHUFFLE_SINK_HANDLE_SIZE);
    metricService.createAutoGauge(
        Metric.DATA_EXCHANGE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        dataExchangeManager,
        MPPDataExchangeManager::getSourceHandleSize,
        Tag.NAME.toString(),
        SOURCE_HANDLE_SIZE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    sendNewDataBlockNumCallerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    sendNewDataBlockNumServerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    onAcknowledgeDataBlockNumCallerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    onAcknowledgeDataBlockNumServerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    getDataBlockNumCallerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    getDataBlockNumServerHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    Arrays.asList(SEND_NEW_DATA_BLOCK_NUM, ON_ACKNOWLEDGE_DATA_BLOCK_NUM, GET_DATA_BLOCK_NUM)
        .forEach(
            name ->
                Arrays.asList(CALLER, SERVER)
                    .forEach(
                        caller ->
                            metricService.remove(
                                MetricType.HISTOGRAM,
                                Metric.DATA_EXCHANGE_COUNT.toString(),
                                Tag.NAME.toString(),
                                name,
                                Tag.TYPE.toString(),
                                caller)));
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DATA_EXCHANGE_SIZE.toString(),
        Tag.NAME.toString(),
        SHUFFLE_SINK_HANDLE_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.DATA_EXCHANGE_SIZE.toString(),
        Tag.NAME.toString(),
        SOURCE_HANDLE_SIZE);
  }

  public void recordDataBlockNum(String type, int num) {
    switch (type) {
      case SEND_NEW_DATA_BLOCK_NUM_CALLER:
        sendNewDataBlockNumCallerHistogram.update(num);
        break;
      case SEND_NEW_DATA_BLOCK_NUM_SERVER:
        sendNewDataBlockNumServerHistogram.update(num);
        break;
      case ON_ACKNOWLEDGE_DATA_BLOCK_NUM_CALLER:
        onAcknowledgeDataBlockNumCallerHistogram.update(num);
        break;
      case ON_ACKNOWLEDGE_DATA_BLOCK_NUM_SERVER:
        onAcknowledgeDataBlockNumServerHistogram.update(num);
        break;
      case GET_DATA_BLOCK_NUM_CALLER:
        getDataBlockNumCallerHistogram.update(num);
        break;
      case GET_DATA_BLOCK_NUM_SERVER:
        getDataBlockNumServerHistogram.update(num);
        break;
      default:
        break;
    }
  }

  public static DataExchangeCountMetricSet getInstance() {
    return INSTANCE;
  }
}
