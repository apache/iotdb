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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.nio.file.FileSystemException;

/** Records DataNode exceptions whose cause chain contains a {@link FileSystemException}. */
public class DataNodeExceptionMetrics implements IMetricSet {

  private static final DataNodeExceptionMetrics INSTANCE = new DataNodeExceptionMetrics();

  private Counter fileSystemExceptionCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private DataNodeExceptionMetrics() {
    // singleton
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    fileSystemExceptionCounter =
        metricService.getOrCreateCounter(
            Metric.FILE_SYSTEM_EXCEPTION_COUNT.toString(), MetricLevel.IMPORTANT);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    fileSystemExceptionCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    metricService.remove(MetricType.COUNTER, Metric.FILE_SYSTEM_EXCEPTION_COUNT.toString());
  }

  public void recordFileSystemException(Throwable throwable) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current instanceof FileSystemException) {
        fileSystemExceptionCounter.inc();
        return;
      }
    }
  }

  public static DataNodeExceptionMetrics getInstance() {
    return INSTANCE;
  }
}
