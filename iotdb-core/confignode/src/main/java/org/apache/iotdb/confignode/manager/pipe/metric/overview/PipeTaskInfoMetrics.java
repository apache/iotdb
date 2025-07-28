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

package org.apache.iotdb.confignode.manager.pipe.metric.overview;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinator;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeTaskInfoMetrics implements IMetricSet {

  private final PipeManager pipeManager;

  private static final String RUNNING = "running";

  private static final String DROPPED = "dropped";

  private static final String USER_STOPPED = "userStopped";

  private static final String EXCEPTION_STOPPED = "exceptionStopped";

  public PipeTaskInfoMetrics(PipeManager pipeManager) {
    this.pipeManager = pipeManager;
  }

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    PipeTaskCoordinator coordinator = pipeManager.getPipeTaskCoordinator();
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::runningPipeCount,
        Tag.STATUS.toString(),
        RUNNING);
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::droppedPipeCount,
        Tag.STATUS.toString(),
        DROPPED);
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::userStoppedPipeCount,
        Tag.STATUS.toString(),
        USER_STOPPED);
    metricService.createAutoGauge(
        Metric.PIPE_TASK_STATUS.toString(),
        MetricLevel.IMPORTANT,
        coordinator,
        PipeTaskCoordinator::exceptionStoppedPipeCount,
        Tag.STATUS.toString(),
        EXCEPTION_STOPPED);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_TASK_STATUS.toString(), Tag.STATUS.toString(), RUNNING);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.PIPE_TASK_STATUS.toString(), Tag.STATUS.toString(), DROPPED);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_TASK_STATUS.toString(),
        Tag.STATUS.toString(),
        USER_STOPPED);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_TASK_STATUS.toString(),
        Tag.STATUS.toString(),
        EXCEPTION_STOPPED);
  }
}
