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
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class DataExchangeCostMetricSet implements IMetricSet {
  private static final DataExchangeCostMetricSet INSTANCE = new DataExchangeCostMetricSet();

  private DataExchangeCostMetricSet() {
    // empty constructor
  }

  // region tsblock related
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
  private Timer sourceHandleGetTsBlockLocalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sourceHandleGetTsBlockRemoteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sourceHandleDeserializeTsBlockLocalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sourceHandleDeserializeTsBlockRemoteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sinkHandleSendTsBlockLocalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sinkHandleSendTsBlockRemoteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindTsBlock(AbstractMetricService metricService) {
    sourceHandleGetTsBlockLocalTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK,
            Tag.TYPE.toString(),
            LOCAL);
    sourceHandleGetTsBlockRemoteTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK,
            Tag.TYPE.toString(),
            REMOTE);
    sourceHandleDeserializeTsBlockLocalTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_GET_TSBLOCK,
            Tag.TYPE.toString(),
            LOCAL);
    sourceHandleDeserializeTsBlockRemoteTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SOURCE_HANDLE_GET_TSBLOCK,
            Tag.TYPE.toString(),
            REMOTE);
    sinkHandleSendTsBlockLocalTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SINK_HANDLE_SEND_TSBLOCK,
            Tag.TYPE.toString(),
            LOCAL);
    sinkHandleSendTsBlockRemoteTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SINK_HANDLE_SEND_TSBLOCK,
            Tag.TYPE.toString(),
            REMOTE);
  }

  private void unbindTsBlock(AbstractMetricService metricService) {
    sourceHandleGetTsBlockLocalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    sourceHandleGetTsBlockRemoteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    sourceHandleDeserializeTsBlockLocalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    sourceHandleDeserializeTsBlockRemoteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    sinkHandleSendTsBlockLocalTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    sinkHandleSendTsBlockRemoteTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK,
            SOURCE_HANDLE_DESERIALIZE_TSBLOCK,
            SINK_HANDLE_SEND_TSBLOCK)
        .forEach(
            operation -> {
              Arrays.asList(LOCAL, REMOTE)
                  .forEach(
                      type -> {
                        metricService.remove(
                            MetricType.TIMER,
                            Metric.DATA_EXCHANGE_COST.toString(),
                            Tag.OPERATION.toString(),
                            operation,
                            Tag.TYPE.toString(),
                            type);
                      });
            });
  }

  // endregion

  // region data block related
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

  private Timer sendNewDataBlockEventCallerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer sendNewDataBlockEventServerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer onAcknowledgeDataBlockEventCallerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer onAcknowledgeDataBlockEventServerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer getDataBlockCallerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer getDataBlockServerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private void bindDataBlock(AbstractMetricService metricService) {
    sendNewDataBlockEventCallerTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SEND_NEW_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            CALLER);
    sendNewDataBlockEventServerTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            SEND_NEW_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            SERVER);
    onAcknowledgeDataBlockEventCallerTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            CALLER);
    onAcknowledgeDataBlockEventServerTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK,
            Tag.TYPE.toString(),
            SERVER);
    getDataBlockCallerTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            GET_DATA_BLOCK_TASK,
            Tag.TYPE.toString(),
            CALLER);
    getDataBlockServerTimer =
        metricService.getOrCreateTimer(
            Metric.DATA_EXCHANGE_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.OPERATION.toString(),
            GET_DATA_BLOCK_TASK,
            Tag.TYPE.toString(),
            SERVER);
  }

  private void unbindDataBlock(AbstractMetricService metricService) {
    sendNewDataBlockEventCallerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    sendNewDataBlockEventServerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    onAcknowledgeDataBlockEventCallerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    onAcknowledgeDataBlockEventServerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    getDataBlockCallerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    getDataBlockServerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    Arrays.asList(
            SEND_NEW_DATA_BLOCK_EVENT_TASK,
            ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK,
            GET_DATA_BLOCK_TASK)
        .forEach(
            operation -> {
              Arrays.asList(CALLER, SERVER)
                  .forEach(
                      type -> {
                        metricService.remove(
                            MetricType.TIMER,
                            Metric.DATA_EXCHANGE_COST.toString(),
                            Tag.OPERATION.toString(),
                            operation,
                            Tag.TYPE.toString(),
                            type);
                      });
            });
  }
  // endregion

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindTsBlock(metricService);
    bindDataBlock(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    unbindTsBlock(metricService);
    unbindDataBlock(metricService);
  }

  public void recordDataExchangeCost(String stage, long costTimeInNanos) {
    switch (stage) {
      case SOURCE_HANDLE_GET_TSBLOCK_LOCAL:
        sourceHandleGetTsBlockLocalTimer.updateNanos(costTimeInNanos);
        break;
      case SOURCE_HANDLE_GET_TSBLOCK_REMOTE:
        sourceHandleGetTsBlockRemoteTimer.updateNanos(costTimeInNanos);
        break;
      case SOURCE_HANDLE_DESERIALIZE_TSBLOCK_LOCAL:
        sourceHandleDeserializeTsBlockLocalTimer.updateNanos(costTimeInNanos);
        break;
      case SOURCE_HANDLE_DESERIALIZE_TSBLOCK_REMOTE:
        sourceHandleDeserializeTsBlockRemoteTimer.updateNanos(costTimeInNanos);
        break;
      case SINK_HANDLE_SEND_TSBLOCK_LOCAL:
        sinkHandleSendTsBlockLocalTimer.updateNanos(costTimeInNanos);
        break;
      case SINK_HANDLE_SEND_TSBLOCK_REMOTE:
        sinkHandleSendTsBlockRemoteTimer.updateNanos(costTimeInNanos);
        break;
      case GET_DATA_BLOCK_TASK_SERVER:
        getDataBlockServerTimer.updateNanos(costTimeInNanos);
        break;
      case GET_DATA_BLOCK_TASK_CALLER:
        getDataBlockCallerTimer.updateNanos(costTimeInNanos);
        break;
      case ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER:
        onAcknowledgeDataBlockEventServerTimer.updateNanos(costTimeInNanos);
        break;
      case ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_CALLER:
        onAcknowledgeDataBlockEventCallerTimer.updateNanos(costTimeInNanos);
        break;
      case SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER:
        sendNewDataBlockEventServerTimer.updateNanos(costTimeInNanos);
        break;
      case SEND_NEW_DATA_BLOCK_EVENT_TASK_CALLER:
        sendNewDataBlockEventCallerTimer.updateNanos(costTimeInNanos);
        break;
      default:
        break;
    }
  }

  public static DataExchangeCostMetricSet getInstance() {
    return INSTANCE;
  }
}
