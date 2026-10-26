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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DataRegionMetrics implements IMetricSet {
  // A database has several data regions but a single memory gauge, which sums up the memory of the
  // regions bound to it. Each region adds itself when binding, and the last region to unbind
  // removes the gauge.
  private static final Map<String, Set<DataRegion>> DATABASE_TO_DATA_REGIONS = new HashMap<>();

  private final DataRegion dataRegion;
  private final String databaseName;

  public DataRegionMetrics(DataRegion dataRegion) {
    this.dataRegion = dataRegion;
    this.databaseName = dataRegion.getDatabaseName();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    synchronized (DATABASE_TO_DATA_REGIONS) {
      Set<DataRegion> dataRegions =
          DATABASE_TO_DATA_REGIONS.computeIfAbsent(
              databaseName, database -> ConcurrentHashMap.newKeySet());
      dataRegions.add(dataRegion);
      metricService.createAutoGauge(
          Metric.MEM.toString(),
          MetricLevel.IMPORTANT,
          dataRegions,
          regions -> regions.stream().mapToLong(DataRegion::getMemCost).sum(),
          Tag.NAME.toString(),
          "database_" + databaseName);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    synchronized (DATABASE_TO_DATA_REGIONS) {
      Set<DataRegion> dataRegions = DATABASE_TO_DATA_REGIONS.get(databaseName);
      if (Objects.nonNull(dataRegions)) {
        dataRegions.remove(dataRegion);
        if (!dataRegions.isEmpty()) {
          return;
        }
        DATABASE_TO_DATA_REGIONS.remove(databaseName);
      }
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.MEM.toString(),
          Tag.NAME.toString(),
          "database_" + databaseName);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataRegionMetrics that = (DataRegionMetrics) o;
    return Objects.equals(dataRegion, that.dataRegion)
        && Objects.equals(databaseName, that.databaseName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataRegion, databaseName);
  }
}
