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

package org.apache.iotdb.db.storageengine.load.metrics;

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ActiveLoadingFilesOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadingFilesOperator.class);

  protected static final String FAILED_PREFIX = "failed - ";
  protected static final String PENDING_PREFIX = "pending - ";

  protected AtomicReference<AbstractMetricService> metricService = new AtomicReference<>();
  private final AtomicReference<String> failedDir = new AtomicReference<>();
  private final Set<String> listeningDirs = new CopyOnWriteArraySet<>();

  protected Counter pendingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  protected Counter failedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  protected Map<String, Counter> fileCounterMap = new ConcurrentHashMap<>();

  public void recordFailedFileCounter(final long number) {
    failedFileCounter.inc(number - failedFileCounter.getCount());
  }

  public void updateFileMetricInTotal(final long number) {
    pendingFileCounter.inc(number - pendingFileCounter.getCount());
  }

  public void updateFileMetricInDir(final String dirName, final long number) {
    final Counter counter = fileCounterMap.get(dirName);
    if (counter == null) {
      LOGGER.warn("Failed to update file counter, dir({}) does not exist", dirName);
      return;
    }
    counter.inc(number - counter.getCount());
  }

  public void updateListeningDirList(final Set<String> fileNameSet) {
    if (fileNameSet.equals(listeningDirs)) {
      return;
    }
    if (metricService.get() == null) {
      return;
    }
    listeningDirs.clear();
    listeningDirs.addAll(fileNameSet);
    unbindListeningDirsCounter(metricService.get());
    rebindFileMapCounter();
  }

  protected void unbindListeningDirsCounter(AbstractMetricService metricService) {
    fileCounterMap
        .keySet()
        .forEach(
            fileName ->
                metricService.remove(
                    MetricType.COUNTER,
                    getMetrics(),
                    Tag.TYPE.toString(),
                    PENDING_PREFIX + fileName));
  }

  private void rebindFileMapCounter() {
    fileCounterMap.clear();
    if (!listeningDirs.isEmpty()) {
      for (String fileName : listeningDirs) {
        fileCounterMap.put(fileName, getOrCreateFileCounter(PENDING_PREFIX + fileName));
      }
    }
  }

  public void updateFailedDir(final String dirName) {
    if (dirName.equals(failedDir.get())) {
      return;
    }
    if (metricService.get() == null) {
      return;
    }
    failedDir.set(dirName);
    unbindFailedDirCounter(metricService.get());
    failedFileCounter = getOrCreateFileCounter(FAILED_PREFIX + failedDir.get());
  }

  protected void unbindFailedDirCounter(final AbstractMetricService metricService) {
    failedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

    metricService.remove(
        MetricType.COUNTER, getMetrics(), Tag.TYPE.toString(), FAILED_PREFIX + failedDir.get());
  }

  private Counter getOrCreateFileCounter(final String fileName) {
    return metricService
        .get()
        .getOrCreateCounter(getMetrics(), MetricLevel.IMPORTANT, Tag.TYPE.toString(), fileName);
  }

  protected abstract void bindFileCounter(final AbstractMetricService metricService);

  protected abstract void unbindFileCounter(final AbstractMetricService metricService);

  protected abstract String getMetrics();
}
