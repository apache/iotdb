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
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class LoadTsFileMemMetricSet implements IMetricSet {

  private static final String LOAD_TSFILE_USED_MEMORY = "LoadTsFileUsedMemory";
  public static final String LOAD_TSFILE_ANALYZE_SCHEMA_MEMORY = "LoadTsFileAnalyzeSchemaMemory";

  private static final String LOAD_TSFILE_DATA_CACHE_MEMORY = "LoadTsFileDataCacheMemory";

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.LOAD_MEM.toString(),
        MetricLevel.IMPORTANT,
        LoadTsFileMemoryManager.getInstance(),
        LoadTsFileMemoryManager::getUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        LOAD_TSFILE_USED_MEMORY);

    metricService.createAutoGauge(
        Metric.LOAD_MEM.toString(),
        MetricLevel.IMPORTANT,
        LoadTsFileMemoryManager.getInstance(),
        LoadTsFileMemoryManager::getDataCacheUsedMemorySizeInBytes,
        Tag.NAME.toString(),
        LOAD_TSFILE_DATA_CACHE_MEMORY);

    metricService
        .getOrCreateGauge(
            Metric.LOAD_MEM.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            LOAD_TSFILE_ANALYZE_SCHEMA_MEMORY)
        .set(0L);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.LOAD_MEM.toString(),
        Tag.NAME.toString(),
        LOAD_TSFILE_USED_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.LOAD_MEM.toString(),
        Tag.NAME.toString(),
        LOAD_TSFILE_DATA_CACHE_MEMORY);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.LOAD_MEM.toString(),
        Tag.NAME.toString(),
        LOAD_TSFILE_ANALYZE_SCHEMA_MEMORY);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class LoadTsFileMemMetricSetHolder {

    private static final LoadTsFileMemMetricSet INSTANCE = new LoadTsFileMemMetricSet();

    private LoadTsFileMemMetricSetHolder() {
      // empty constructor
    }
  }

  public static LoadTsFileMemMetricSet getInstance() {
    return LoadTsFileMemMetricSet.LoadTsFileMemMetricSetHolder.INSTANCE;
  }

  private LoadTsFileMemMetricSet() {
    // empty constructor
  }
}
