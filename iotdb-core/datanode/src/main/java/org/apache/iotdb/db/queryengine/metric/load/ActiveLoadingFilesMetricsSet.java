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

package org.apache.iotdb.db.queryengine.metric.load;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.queryengine.execution.load.active.ActiveLoadListeningDirsCountExecutor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class ActiveLoadingFilesMetricsSet implements IMetricSet {

  private static final ActiveLoadingFilesMetricsSet INSTANCE = new ActiveLoadingFilesMetricsSet();

  public static final String PENDING_UNPROCESS_FILE = "pendingUnprocessFile";
  public static final String QUEUING_FILE = "queuingFile";
  public static final String LOADING_FILE = "loadingFile";
  public static final String FAILED_FILE = "failedFile";

  private ActiveLoadingFilesMetricsSet() {
    // empty construct
  }

  private Counter pendingUnprocessFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter queuingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter loadingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter failedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  public void recordPendingUnprocessFileCounter(final long number) {
    pendingUnprocessFileCounter.inc(number - pendingUnprocessFileCounter.getCount());
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
    pendingUnprocessFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            PENDING_UNPROCESS_FILE,
            Tag.TYPE.toString(),
            PENDING_UNPROCESS_FILE);
    queuingFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            QUEUING_FILE,
            Tag.TYPE.toString(),
            QUEUING_FILE);
    loadingFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LOADING_FILE,
            Tag.TYPE.toString(),
            LOADING_FILE);
    failedFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            FAILED_FILE,
            Tag.TYPE.toString(),
            FAILED_FILE);

    ActiveLoadListeningDirsCountExecutor.getInstance().start();
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    pendingUnprocessFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    queuingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    loadingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    failedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.COUNTER,
        Metric.ACTIVE_LOADING_FILES.toString(),
        Tag.NAME.toString(),
        PENDING_UNPROCESS_FILE,
        Tag.TYPE.toString(),
        PENDING_UNPROCESS_FILE);
    metricService.remove(
        MetricType.COUNTER,
        Metric.ACTIVE_LOADING_FILES.toString(),
        Tag.NAME.toString(),
        QUEUING_FILE,
        Tag.TYPE.toString(),
        QUEUING_FILE);
    metricService.remove(
        MetricType.COUNTER,
        Metric.ACTIVE_LOADING_FILES.toString(),
        Tag.NAME.toString(),
        LOADING_FILE,
        Tag.TYPE.toString(),
        LOADING_FILE);
    metricService.remove(
        MetricType.COUNTER,
        Metric.ACTIVE_LOADING_FILES.toString(),
        Tag.NAME.toString(),
        FAILED_FILE,
        Tag.TYPE.toString(),
        FAILED_FILE);

    ActiveLoadListeningDirsCountExecutor.getInstance().stop();
  }

  public static ActiveLoadingFilesMetricsSet getInstance() {
    return ActiveLoadingFilesMetricsSet.INSTANCE;
  }
}
