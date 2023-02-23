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

import java.util.concurrent.atomic.AtomicLong;

/** This class is used to record statistics within the SchemaRegion in Memory mode */
public class MemSchemaRegionStatistics implements ISchemaRegionStatistics {

  protected MemSchemaEngineStatistics schemaEngineStatistics;
  private final int schemaRegionId;
  private final AtomicLong memoryUsage = new AtomicLong(0);
  private final AtomicLong seriesNumber = new AtomicLong(0);

  private long mLogLength = 0;

  public MemSchemaRegionStatistics(int schemaRegionId, ISchemaEngineStatistics engineStatistics) {
    this.schemaEngineStatistics = engineStatistics.getAsMemSchemaEngineStatistics();
    this.schemaRegionId = schemaRegionId;
  }

  @Override
  public boolean isAllowToCreateNewSeries() {
    return schemaEngineStatistics.isAllowToCreateNewSeries();
  }

  public void requestMemory(long size) {
    memoryUsage.addAndGet(size);
    schemaEngineStatistics.requestMemory(size);
  }

  public void releaseMemory(long size) {
    memoryUsage.addAndGet(-size);
    schemaEngineStatistics.releaseMemory(size);
  }

  @Override
  public long getSeriesNumber() {
    return seriesNumber.get();
  }

  public void addTimeseries(long addedNum) {
    seriesNumber.addAndGet(addedNum);
    schemaEngineStatistics.addTimeseries(addedNum);
  }

  public void deleteTimeseries(long deletedNum) {
    seriesNumber.addAndGet(-deletedNum);
    schemaEngineStatistics.deleteTimeseries(deletedNum);
  }

  @Override
  public long getRegionMemoryUsage() {
    return memoryUsage.get();
  }

  @Override
  public int getSchemaRegionId() {
    return schemaRegionId;
  }

  public void setMLogLength(long mLogLength) {
    this.mLogLength = mLogLength;
  }

  public long getMLogLength() {
    return mLogLength;
  }

  @Override
  public MemSchemaRegionStatistics getAsMemSchemaRegionStatistics() {
    return this;
  }

  @Override
  public CachedSchemaRegionStatistics getAsCachedSchemaRegionStatistics() {
    throw new UnsupportedOperationException("Wrong SchemaRegionStatistics Type");
  }

  @Override
  public void clear() {
    schemaEngineStatistics.releaseMemory(memoryUsage.get());
    schemaEngineStatistics.deleteTimeseries(seriesNumber.get());
    memoryUsage.getAndSet(0);
    seriesNumber.getAndSet(0);
  }
}
