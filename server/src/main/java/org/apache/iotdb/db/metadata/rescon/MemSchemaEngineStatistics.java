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
package org.apache.iotdb.db.metadata.rescon;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/** This class is used to record the global statistics of SchemaEngine in Memory mode */
public class MemSchemaEngineStatistics implements ISchemaEngineStatistics {

  private static final Logger logger = LoggerFactory.getLogger(MemSchemaEngineStatistics.class);

  /** Total size of schema region */
  private final long memoryCapacity =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion();

  protected final AtomicLong memoryUsage = new AtomicLong(0);

  private final AtomicLong totalSeriesNumber = new AtomicLong(0);

  private volatile boolean allowToCreateNewSeries = true;

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

  public void requestMemory(long size) {
    memoryUsage.addAndGet(size);
    if (memoryUsage.get() >= memoryCapacity) {
      logger.warn("Current series memory {} is too large...", memoryUsage);
      allowToCreateNewSeries = false;
    }
  }

  public void releaseMemory(long size) {
    memoryUsage.addAndGet(-size);
    if (!allowToCreateNewSeries && memoryUsage.get() < memoryCapacity) {
      logger.info(
          "Current series memory {} come back to normal level, total series number is {}.",
          memoryUsage,
          totalSeriesNumber);
      allowToCreateNewSeries = true;
    }
  }

  @Override
  public long getTotalSeriesNumber() {
    return totalSeriesNumber.get();
  }

  @Override
  public int getSchemaRegionNumber() {
    return SchemaEngine.getInstance().getSchemaRegionNumber();
  }

  public void addTimeseries(long addedNum) {
    totalSeriesNumber.addAndGet(addedNum);
  }

  public void deleteTimeseries(long deletedNum) {
    totalSeriesNumber.addAndGet(-deletedNum);
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
