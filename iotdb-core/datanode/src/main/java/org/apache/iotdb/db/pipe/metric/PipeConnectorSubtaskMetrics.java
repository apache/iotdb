/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PipeConnectorSubtaskMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtaskMetrics.class);

  private AbstractMetricService metricService;

  private final Map<String, PipeConnectorSubtask> taskMap = new HashMap<>();

  private static class PipeConnectorSubtaskMetricsHolder {

    private static final PipeConnectorSubtaskMetrics INSTANCE = new PipeConnectorSubtaskMetrics();

    private PipeConnectorSubtaskMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeConnectorSubtaskMetrics getInstance() {
    return PipeConnectorSubtaskMetrics.PipeConnectorSubtaskMetricsHolder.INSTANCE;
  }

  private PipeConnectorSubtaskMetrics() {
    // empty constructor
  }

  public void register(PipeConnectorSubtask pipeConnectorSubtask) {
    String taskID = pipeConnectorSubtask.getTaskID();
    synchronized (this) {
      if (metricService == null) {
        taskMap.put(taskID, pipeConnectorSubtask);
      } else {
        if (!taskMap.containsKey(taskID)) {
          taskMap.put(taskID, pipeConnectorSubtask);
          createMetrics(taskID);
        }
      }
    }
  }

  private void createMetrics(String taskID) {
    metricService.createAutoGauge(
        Metric.TABLET_INSERTION_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getTabletInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
    metricService.createAutoGauge(
        Metric.TS_FILE_INSERTION_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        taskMap.get(taskID),
        PipeConnectorSubtask::getTsFileInsertionEventCount,
        Tag.NAME.toString(),
        taskID);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String taskID : taskMap.keySet()) {
        createMetrics(taskID);
      }
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    synchronized (this) {
      for (String taskID : taskMap.keySet()) {
        metricService.remove(
            MetricType.GAUGE,
            Metric.TABLET_INSERTION_EVENT_COUNT.toString(),
            Tag.NAME.toString(),
            taskID);
        metricService.remove(
            MetricType.GAUGE,
            Metric.TS_FILE_INSERTION_EVENT_COUNT.toString(),
            Tag.NAME.toString(),
            taskID);
      }
      taskMap.clear();
    }
  }

  public void deregister(String taskID) {
    synchronized (this) {
      if (!taskMap.containsKey(taskID)) {
        LOGGER.info(
            String.format(
                "Failed to deregister pipe connector subtask metrics, %s does not exist", taskID));
        return;
      }
      metricService.remove(
          MetricType.GAUGE,
          Metric.TABLET_INSERTION_EVENT_COUNT.toString(),
          Tag.NAME.toString(),
          taskID);
      metricService.remove(
          MetricType.GAUGE,
          Metric.TS_FILE_INSERTION_EVENT_COUNT.toString(),
          Tag.NAME.toString(),
          taskID);
      taskMap.remove(taskID);
    }
  }
}
