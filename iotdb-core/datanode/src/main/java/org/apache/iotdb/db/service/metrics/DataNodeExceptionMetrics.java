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

import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.file.FileSystemException;

/**
 * Records DataNode exceptions whose cause chain contains a suspicious disk I/O exception, i.e. an
 * exact {@link FileSystemException}, a {@link SyncFailedException}, or an {@link IOException}
 * carrying a well-known disk-error message.
 */
public class DataNodeExceptionMetrics implements IMetricSet {

  private static final DataNodeExceptionMetrics INSTANCE = new DataNodeExceptionMetrics();

  private Counter suspiciousDiskExceptionCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private DataNodeExceptionMetrics() {
    // singleton
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    suspiciousDiskExceptionCounter =
        metricService.getOrCreateCounter(
            Metric.SUSPICIOUS_DISK_EXCEPTION_COUNT.toString(), MetricLevel.IMPORTANT);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    suspiciousDiskExceptionCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    metricService.remove(MetricType.COUNTER, Metric.SUSPICIOUS_DISK_EXCEPTION_COUNT.toString());
  }

  public void recordSuspiciousDiskException(Throwable throwable) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (isSuspiciousDiskException(current)) {
        suspiciousDiskExceptionCounter.inc();
        return;
      }
    }
  }

  private static boolean isSuspiciousDiskException(Throwable throwable) {
    // 1) exact FileSystemException. Subclasses (e.g. NoSuchFileException,
    // NotDirectoryException, AccessDeniedException) usually indicate logical
    // file-state errors rather than real disk failures, so they are not counted here.
    if (throwable.getClass() == FileSystemException.class) {
      return true;
    }
    // 2) SyncFailedException: fsync/force failure is almost always a real disk problem.
    // Its message is fixed to "Sync failed" without the errno text, so it can only be
    // recognized by its type.
    if (throwable instanceof SyncFailedException) {
      return true;
    }
    // 3) Plain IOException thrown by reads/writes/force on already-open channels carries
    // the errno text in its message. Match the well-known disk-error messages instead of
    // counting every IOException (EOF, closed stream, etc. are not disk problems).
    if (throwable instanceof IOException) {
      String message = throwable.getMessage();
      if (message != null) {
        return message.contains("Input/output error")
            || message.contains("No space left on device")
            || message.contains("Read-only file system")
            || message.contains("Structure needs cleaning")
            || message.contains("No such device or address")
            || message.contains("Disk quota exceeded");
      }
    }
    return false;
  }

  public static DataNodeExceptionMetrics getInstance() {
    return INSTANCE;
  }
}
