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

package org.apache.iotdb.db.storageengine.load.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class ActiveLoadingFilesMetricsSet implements IMetricSet {

  private static final ActiveLoadingFilesMetricsSet INSTANCE = new ActiveLoadingFilesMetricsSet();

  public static final String PENDING = "pending";
  public static final String QUEUING = "queuing";
  public static final String LOADING = "loading";
  public static final String FAILED = "failed";

  private ActiveLoadingFilesMetricsSet() {
    // empty construct
  }

  private Counter pendingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter queuingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter loadingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter failedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  public void recordPendingFileCounter(final long number) {
    pendingFileCounter.inc(number - pendingFileCounter.getCount());
  }

  public void recordQueuingFileCounter(final long number) {
    queuingFileCounter.inc(number);
  }

  public void recordLoadingFileCounter(final long number) {
    loadingFileCounter.inc(number);
  }

  public void recordFailedFileCounter(final long number) {
    failedFileCounter.inc(number - failedFileCounter.getCount());
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    pendingFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            PENDING);
    queuingFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            QUEUING);
    loadingFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            LOADING);
    failedFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            FAILED);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    pendingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    queuingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    loadingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    failedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.COUNTER, Metric.ACTIVE_LOADING_FILES.toString(), Tag.TYPE.toString(), PENDING);
    metricService.remove(
        MetricType.COUNTER, Metric.ACTIVE_LOADING_FILES.toString(), Tag.TYPE.toString(), QUEUING);
    metricService.remove(
        MetricType.COUNTER, Metric.ACTIVE_LOADING_FILES.toString(), Tag.TYPE.toString(), LOADING);
    metricService.remove(
        MetricType.COUNTER, Metric.ACTIVE_LOADING_FILES.toString(), Tag.TYPE.toString(), FAILED);
  }

  public static ActiveLoadingFilesMetricsSet getInstance() {
    return ActiveLoadingFilesMetricsSet.INSTANCE;
  }
}
