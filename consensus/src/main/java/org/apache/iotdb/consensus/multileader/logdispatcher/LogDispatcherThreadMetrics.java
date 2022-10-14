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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class LogDispatcherThreadMetrics implements IMetricSet {
  private final ConsensusGroupId consensusGroupId;
  private final LogDispatcher.LogDispatcherThread logDispatcherThread;

  public LogDispatcherThreadMetrics(
      ConsensusGroupId consensusGroupId, LogDispatcher.LogDispatcherThread logDispatcherThread) {
    this.consensusGroupId = consensusGroupId;
    this.logDispatcherThread = logDispatcherThread;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.LOG_DISPATCHER.toString(),
            MetricLevel.IMPORTANT,
            logDispatcherThread,
            LogDispatcher.LogDispatcherThread::getPendingRequestSize,
            Tag.NAME.toString(),
            "[DataRegion-"
                + consensusGroupId
                + "]"
                + formatEndPoint(logDispatcherThread.getPeer().getEndpoint()),
            Tag.TYPE.toString(),
            "PendingRequestSize");
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.LOG_DISPATCHER.toString(),
            MetricLevel.IMPORTANT,
            logDispatcherThread,
            LogDispatcher.LogDispatcherThread::getBufferRequestSize,
            Tag.NAME.toString(),
            "[DataRegion-"
                + consensusGroupId
                + "]"
                + formatEndPoint(logDispatcherThread.getPeer().getEndpoint()),
            Tag.TYPE.toString(),
            "BufferRequestSize");
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.LOG_DISPATCHER.toString(),
            MetricLevel.IMPORTANT,
            logDispatcherThread,
            LogDispatcher.LogDispatcherThread::getCurrentSyncIndex,
            Tag.NAME.toString(),
            "[DataRegion-"
                + consensusGroupId
                + "]"
                + formatEndPoint(logDispatcherThread.getPeer().getEndpoint()),
            Tag.TYPE.toString(),
            "currentIndex");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.LOG_DISPATCHER.toString(),
            Tag.NAME.toString(),
            "[DataRegion-"
                + consensusGroupId
                + "]"
                + formatEndPoint(logDispatcherThread.getPeer().getEndpoint()),
            Tag.TYPE.toString(),
            "PendingRequestSize");
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.LOG_DISPATCHER.toString(),
            Tag.NAME.toString(),
            "[DataRegion-"
                + consensusGroupId
                + "]"
                + formatEndPoint(logDispatcherThread.getPeer().getEndpoint()),
            Tag.TYPE.toString(),
            "BufferRequestSize");
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.LOG_DISPATCHER.toString(),
            Tag.NAME.toString(),
            "[DataRegion-"
                + consensusGroupId
                + "]"
                + formatEndPoint(logDispatcherThread.getPeer().getEndpoint()),
            Tag.TYPE.toString(),
            "currentIndex");
  }

  private String formatEndPoint(TEndPoint endPoint) {
    return endPoint.getIp() + ":" + endPoint.getPort();
  }
}
