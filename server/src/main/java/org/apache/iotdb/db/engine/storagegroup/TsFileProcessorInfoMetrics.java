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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;

public class TsFileProcessorInfoMetrics implements IMetricSet {
  private String storageGroupName;
  private long memCost;

  public TsFileProcessorInfoMetrics(String storageGroupName, long memCost) {
    this.storageGroupName = storageGroupName;
    this.memCost = memCost;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    MetricService.getInstance()
        .getOrCreateAutoGauge(
            Metric.MEM.toString(),
            MetricLevel.IMPORTANT,
            memCost,
            o -> o,
            Tag.NAME.toString(),
            "chunkMetaData_" + storageGroupName);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.MEM.toString(),
            Tag.NAME.toString(),
            "chunkMetaData_" + storageGroupName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TsFileProcessorInfoMetrics that = (TsFileProcessorInfoMetrics) o;
    return memCost == that.memCost && Objects.equals(storageGroupName, that.storageGroupName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroupName, memCost);
  }
}
