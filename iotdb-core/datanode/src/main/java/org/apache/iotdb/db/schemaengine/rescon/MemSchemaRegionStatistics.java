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

import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** This class is used to record statistics within the SchemaRegion in Memory mode. */
public class MemSchemaRegionStatistics implements ISchemaRegionStatistics {

  protected MemSchemaEngineStatistics schemaEngineStatistics;
  private final int schemaRegionId;
  private final AtomicLong memoryUsage = new AtomicLong(0);
  // normal series number, not include view and template series
  private final AtomicLong measurementNumber = new AtomicLong(0);
  private final AtomicLong devicesNumber = new AtomicLong(0);
  private final AtomicLong viewNumber = new AtomicLong(0);
  private final Map<Integer, Integer> templateUsage = new ConcurrentHashMap<>();

  private long mLogLength = 0;

  public MemSchemaRegionStatistics(
      final int schemaRegionId, final ISchemaEngineStatistics engineStatistics) {
    this.schemaEngineStatistics = engineStatistics.getAsMemSchemaEngineStatistics();
    this.schemaRegionId = schemaRegionId;
  }

  @Override
  public boolean isAllowToCreateNewSeries() {
    return schemaEngineStatistics.isAllowToCreateNewSeries();
  }

  public void requestMemory(final long size) {
    memoryUsage.addAndGet(size);
    schemaEngineStatistics.requestMemory(size);
  }

  public void releaseMemory(final long size) {
    memoryUsage.addAndGet(-size);
    schemaEngineStatistics.releaseMemory(size);
  }

  @Override
  public long getSeriesNumber(final boolean includeView) {
    if (includeView) {
      return viewNumber.get() + measurementNumber.get() + getTemplateSeriesNumber();
    } else {
      return measurementNumber.get() + getTemplateSeriesNumber();
    }
  }

  public void addMeasurement(final long addedNum) {
    measurementNumber.addAndGet(addedNum);
    schemaEngineStatistics.addMeasurement(addedNum);
  }

  public void deleteMeasurement(long deletedNum) {
    measurementNumber.addAndGet(-deletedNum);
    schemaEngineStatistics.deleteMeasurement(deletedNum);
  }

  public void addView(final long addedNum) {
    viewNumber.addAndGet(addedNum);
    schemaEngineStatistics.addView(addedNum);
  }

  public void deleteView(final long deletedNum) {
    viewNumber.addAndGet(-deletedNum);
    schemaEngineStatistics.deleteView(deletedNum);
  }

  @Override
  public long getDevicesNumber() {
    return devicesNumber.get();
  }

  public void addDevice() {
    devicesNumber.incrementAndGet();
    schemaEngineStatistics.addDevice();
  }

  public void deleteDevice() {
    devicesNumber.decrementAndGet();
    schemaEngineStatistics.deleteDevice(1L);
  }

  @Override
  public int getTemplateActivatedNumber() {
    return templateUsage.size();
  }

  @Override
  public long getTemplateSeriesNumber() {
    final ClusterTemplateManager clusterTemplateManager = ClusterTemplateManager.getInstance();
    return templateUsage.entrySet().stream()
        .mapToLong(
            i -> {
              Template t = clusterTemplateManager.getTemplate(i.getKey());
              return t == null ? 0 : (long) t.getMeasurementNumber() * i.getValue();
            })
        .sum();
  }

  public void activateTemplate(final int templateId) {
    templateUsage.compute(templateId, (k, v) -> (v == null) ? 1 : v + 1);
    schemaEngineStatistics.activateTemplate(templateId);
  }

  public void deactivateTemplate(final int templateId) {
    templateUsage.compute(templateId, (k, v) -> (v == null || v == 1) ? null : v - 1);
    schemaEngineStatistics.deactivateTemplate(templateId, 1);
  }

  @Override
  public long getRegionMemoryUsage() {
    return memoryUsage.get();
  }

  @Override
  public int getSchemaRegionId() {
    return schemaRegionId;
  }

  public void setMLogLength(final long mLogLength) {
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
    schemaEngineStatistics.deleteMeasurement(measurementNumber.get());
    schemaEngineStatistics.deleteView(viewNumber.get());
    schemaEngineStatistics.deleteDevice(devicesNumber.get());
    memoryUsage.getAndSet(0);
    measurementNumber.getAndSet(0);
    devicesNumber.getAndSet(0);
    viewNumber.getAndAdd(0);
    templateUsage.forEach(
        (templateId, cnt) -> schemaEngineStatistics.deactivateTemplate(templateId, cnt));
    templateUsage.clear();
  }

  @Override
  public long getGlobalMemoryUsage() {
    return schemaEngineStatistics.getMemoryUsage();
  }

  @Override
  public long getGlobalSeriesNumber() {
    return schemaEngineStatistics.getTotalSeriesNumber();
  }
}
