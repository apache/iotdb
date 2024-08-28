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
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class ActiveLoadingFilesSizeMetricsSet extends ActiveLoadingFilesOperator
    implements IMetricSet {

  private static final ActiveLoadingFilesSizeMetricsSet INSTANCE =
      new ActiveLoadingFilesSizeMetricsSet();

  private static final String PENDING_SIZE = "pending (total)";

  private ActiveLoadingFilesSizeMetricsSet() {
    // empty construct
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService.set(metricService);
    bindFileCounter(metricService);
  }

  @Override
  protected void bindFileCounter(AbstractMetricService metricService) {
    pendingFileCounter =
        metricService.getOrCreateCounter(
            Metric.ACTIVE_LOADING_FILES_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            PENDING_SIZE);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    unbindFileCounter(metricService);
    unbindListeningDirsCounter(metricService);
    unbindFailedDirCounter(metricService);
  }

  @Override
  protected void unbindFileCounter(AbstractMetricService metricService) {
    pendingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.COUNTER,
        Metric.ACTIVE_LOADING_FILES_SIZE.toString(),
        Tag.TYPE.toString(),
        PENDING_SIZE);
  }

  @Override
  protected String getMetrics() {
    return Metric.ACTIVE_LOADING_FILES_SIZE.toString();
  }

  public static ActiveLoadingFilesSizeMetricsSet getInstance() {
    return INSTANCE;
  }
}
