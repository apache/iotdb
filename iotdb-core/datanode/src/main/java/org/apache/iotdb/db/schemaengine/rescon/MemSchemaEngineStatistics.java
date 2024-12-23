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

package org.apache.iotdb.db.schemaengine.rescon;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** This class is used to record the global statistics of SchemaEngine in Memory mode. */
public class MemSchemaEngineStatistics implements ISchemaEngineStatistics {

  private static final Logger logger = LoggerFactory.getLogger(MemSchemaEngineStatistics.class);

  // Total size of schema region
  private final long memoryCapacity =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion();
  private final ClusterTemplateManager clusterTemplateManager =
      ClusterTemplateManager.getInstance();

  protected final AtomicLong memoryUsage = new AtomicLong(0);
  private final AtomicLong totalMeasurementNumber = new AtomicLong(0);
  private final AtomicLong totalViewNumber = new AtomicLong(0);
  private final AtomicLong totalDeviceNumber = new AtomicLong(0);
  private final Map<Integer, Integer> templateUsage = new ConcurrentHashMap<>();
  private volatile boolean allowToCreateNewSeries = true;

  private final Object allowToCreateNewSeriesLock = new Object();

  // In memory mode, we need log to warn users that the memory capacity has been reached.
  // In pbtree mode, we only need related log for debug.
  protected volatile boolean needLog = true;

  @Override
  public boolean isAllowToCreateNewSeries() {
    return allowToCreateNewSeries;
  }

  @Override
  public boolean isExceedCapacity() {
    return memoryUsage.get() > memoryCapacity;
  }

  @Override
  public long getMemoryCapacity() {
    return memoryCapacity;
  }

  @Override
  public long getMemoryUsage() {
    return memoryUsage.get();
  }

  public void requestMemory(final long size) {
    memoryUsage.addAndGet(size);
    if (memoryUsage.get() >= memoryCapacity) {
      synchronized (allowToCreateNewSeriesLock) {
        if (allowToCreateNewSeries && memoryUsage.get() >= memoryCapacity) {
          if (needLog) {
            logger.warn("Current series memory {} is too large...", memoryUsage);
          } else {
            logger.debug("Current series memory {} is too large...", memoryUsage);
          }
          allowToCreateNewSeries = false;
        }
      }
    }
  }

  public void releaseMemory(final long size) {
    memoryUsage.addAndGet(-size);
    if (memoryUsage.get() < memoryCapacity) {
      synchronized (allowToCreateNewSeriesLock) {
        if (!allowToCreateNewSeries && memoryUsage.get() < memoryCapacity) {
          if (needLog) {
            logger.info(
                "Current series memory {} come back to normal level, total series number is {}.",
                memoryUsage,
                totalMeasurementNumber.get() + totalViewNumber.get());
          } else {
            logger.debug(
                "Current series memory {} come back to normal level, total series number is {}.",
                memoryUsage,
                totalMeasurementNumber.get() + totalViewNumber.get());
          }
          allowToCreateNewSeries = true;
        }
      }
    }
  }

  @Override
  public long getTotalSeriesNumber() {
    return totalMeasurementNumber.get() + totalViewNumber.get() + getTemplateSeriesNumber();
  }

  @Override
  public long getTotalDevicesNumber() {
    return totalDeviceNumber.get();
  }

  @Override
  public int getSchemaRegionNumber() {
    return SchemaEngine.getInstance().getSchemaRegionNumber();
  }

  @Override
  public long getTemplateSeriesNumber() {
    return templateUsage.entrySet().stream()
        .mapToLong(
            i ->
                (long) clusterTemplateManager.getTemplate(i.getKey()).getMeasurementNumber()
                    * i.getValue())
        .sum();
  }

  @Override
  public int getTemplateUsingNumber(String templateName) {
    Integer templateId = clusterTemplateManager.getTemplateId(templateName);
    if (templateId == null) {
      return 0;
    } else {
      return templateUsage.getOrDefault(templateId, 0);
    }
  }

  public void activateTemplate(int templateId) {
    templateUsage.compute(templateId, (k, v) -> (v == null) ? 1 : v + 1);
  }

  public void deactivateTemplate(int templateId, int cnt) {
    templateUsage.compute(templateId, (k, v) -> (v == null || v <= cnt) ? null : v - cnt);
  }

  public void addMeasurement(long addedNum) {
    totalMeasurementNumber.addAndGet(addedNum);
  }

  public void deleteMeasurement(long deletedNum) {
    totalMeasurementNumber.addAndGet(-deletedNum);
  }

  public void addView(long addedNum) {
    totalViewNumber.addAndGet(addedNum);
  }

  public void deleteView(long deletedNum) {
    totalViewNumber.addAndGet(-deletedNum);
  }

  public void addDevice() {
    totalDeviceNumber.incrementAndGet();
  }

  public void deleteDevice(long cnt) {
    totalDeviceNumber.addAndGet(-cnt);
  }

  @Override
  public MemSchemaEngineStatistics getAsMemSchemaEngineStatistics() {
    return this;
  }

  @Override
  public CachedSchemaEngineStatistics getAsCachedSchemaEngineStatistics() {
    throw new UnsupportedOperationException("Wrong SchemaEngineStatistics Type");
  }
}
