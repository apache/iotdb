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
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ActiveLoadingFilesMetricsSet implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadingFilesMetricsSet.class);

  protected static final String FAILED_PREFIX = "failed - ";
  protected static final String PENDING_PREFIX = "pending - ";

  protected AtomicReference<AbstractMetricService> metricService = new AtomicReference<>();

  private final AtomicReference<String> failedDir = new AtomicReference<>();
  private final Set<String> pendingDirs = new CopyOnWriteArraySet<>();

  protected Counter totalFailedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  protected Counter totalPendingFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  protected Map<String, Counter> dir2PendingFileCounterMap = new ConcurrentHashMap<>();

  public void updateTotalFailedFileCounter(final long number) {
    totalFailedFileCounter.inc(number - totalFailedFileCounter.getCount());
  }

  public void updateTotalPendingFileCounter(final long number) {
    totalPendingFileCounter.inc(number - totalPendingFileCounter.getCount());
  }

  public void updatePendingFileCounterInDir(final String dirName, final long number) {
    final Counter counter = dir2PendingFileCounterMap.get(dirName);
    if (counter == null) {
      LOGGER.debug("Failed to update file counter, dir({}) does not exist", dirName);
      return;
    }
    counter.inc(number - counter.getCount());
  }

  public void updatePendingDirList(final Set<String> givenListeningDirs) {
    if (metricService.get() == null || Objects.equals(pendingDirs, givenListeningDirs)) {
      return;
    }

    pendingDirs.clear();
    pendingDirs.addAll(givenListeningDirs);

    unbindDir2PendingFileCounters(metricService.get());
    rebindDir2PendingFileCounters();
  }

  protected void unbindDir2PendingFileCounters(final AbstractMetricService metricService) {
    dir2PendingFileCounterMap
        .keySet()
        .forEach(
            dir ->
                metricService.remove(
                    MetricType.COUNTER,
                    getMetricName(),
                    Tag.TYPE.toString(),
                    PENDING_PREFIX + dir));
    dir2PendingFileCounterMap.clear();
  }

  private void rebindDir2PendingFileCounters() {
    dir2PendingFileCounterMap.clear();
    if (!pendingDirs.isEmpty()) {
      for (String dir : pendingDirs) {
        dir2PendingFileCounterMap.put(
            dir,
            metricService
                .get()
                .getOrCreateCounter(
                    getMetricName(),
                    MetricLevel.IMPORTANT,
                    Tag.TYPE.toString(),
                    PENDING_PREFIX + dir));
      }
    }
  }

  public void updateFailedDir(final String dirName) {
    if (metricService.get() == null || Objects.equals(failedDir.get(), dirName)) {
      return;
    }

    failedDir.set(dirName);

    unbindFailedDirCounter(metricService.get());
    rebindFailedDirCounter();
  }

  protected void unbindFailedDirCounter(final AbstractMetricService metricService) {
    totalFailedFileCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    metricService.remove(
        MetricType.COUNTER, getMetricName(), Tag.TYPE.toString(), FAILED_PREFIX + failedDir.get());
  }

  private void rebindFailedDirCounter() {
    totalFailedFileCounter =
        metricService
            .get()
            .getOrCreateCounter(
                getMetricName(),
                MetricLevel.IMPORTANT,
                Tag.TYPE.toString(),
                FAILED_PREFIX + failedDir.get());
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService.set(metricService);

    // Dir2PendingFileCounters' binding is triggered by updatePendingDirList
    // FailedDirCounter's binding is triggered by updateFailedDir
    bindOtherCounters(metricService);
  }

  protected abstract void bindOtherCounters(final AbstractMetricService metricService);

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    unbindDir2PendingFileCounters(metricService);
    unbindFailedDirCounter(metricService);
    unbindOtherCounters(metricService);
  }

  protected abstract void unbindOtherCounters(final AbstractMetricService metricService);

  protected abstract String getMetricName();
}
