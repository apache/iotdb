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
package org.apache.iotdb.db.metadata.metric;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mtree.store.disk.cache.CacheMemoryManager;
import org.apache.iotdb.db.metadata.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaMetricManager {

  private final Map<Integer, ISchemaRegionMetric> schemaRegionMetricMap = new ConcurrentHashMap<>();
  private ISchemaEngineMetric engineMetric;

  private SchemaMetricManager() {}

  public void init(ISchemaEngineStatistics engineStatistics) {
    if (IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode().equals("Memory")) {
      engineMetric = new SchemaEngineMemMetric(engineStatistics.getAsMemSchemaEngineStatistics());
    } else {
      SchemaEngineCachedMetric schemaEngineCachedMetric =
          new SchemaEngineCachedMetric(engineStatistics.getAsCachedSchemaEngineStatistics());
      engineMetric = schemaEngineCachedMetric;
      CacheMemoryManager.getInstance().setEngineMetric(schemaEngineCachedMetric);
    }
    MetricService.getInstance().addMetricSet(engineMetric);
  }

  public void createSchemaRegionMetric(ISchemaRegion schemaRegion) {
    ISchemaRegionMetric schemaRegionMetric = schemaRegion.createSchemaRegionMetric();
    schemaRegionMetricMap.put(schemaRegion.getSchemaRegionId().getId(), schemaRegionMetric);
    MetricService.getInstance().addMetricSet(schemaRegionMetric);
  }

  public void deleteSchemaRegionMetric(int schemaRegionId) {
    ISchemaRegionMetric schemaRegionMetric = schemaRegionMetricMap.remove(schemaRegionId);
    if (schemaRegionMetric != null) {
      MetricService.getInstance().removeMetricSet(schemaRegionMetric);
    }
  }

  public void clear() {
    if (engineMetric != null) {
      MetricService.getInstance().removeMetricSet(engineMetric);
      engineMetric = null;
    }
    for (ISchemaRegionMetric regionMetric : schemaRegionMetricMap.values()) {
      MetricService.getInstance().removeMetricSet(regionMetric);
    }
    schemaRegionMetricMap.clear();
  }

  /** SingleTone */
  private static class SchemaMetricManagerHolder {
    private static final SchemaMetricManager INSTANCE = new SchemaMetricManager();

    private SchemaMetricManagerHolder() {
      // Empty constructor
    }
  }

  public static SchemaMetricManager getInstance() {
    return SchemaMetricManager.SchemaMetricManagerHolder.INSTANCE;
  }
}
