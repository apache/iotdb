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
package org.apache.iotdb.db.storageengine.dataregion.memtable;

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

public class TsFileProcessorInfoMetrics implements IMetricSet {
  // A database has many TsFileProcessors but a single chunk metadata memory gauge, which sums up
  // the memory of the processors bound to it. Each processor adds itself when binding, and the last
  // processor to unbind removes the gauge.
  private static final Map<String, Set<TsFileProcessorInfo>> DATABASE_TO_PROCESSOR_INFOS =
      new HashMap<>();

  private final String databaseName;
  private final TsFileProcessorInfo tsFileProcessorInfo;

  public TsFileProcessorInfoMetrics(
      String storageGroupName, TsFileProcessorInfo tsFileProcessorInfo) {
    this.databaseName = storageGroupName;
    this.tsFileProcessorInfo = tsFileProcessorInfo;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    synchronized (DATABASE_TO_PROCESSOR_INFOS) {
      Set<TsFileProcessorInfo> processorInfos =
          DATABASE_TO_PROCESSOR_INFOS.computeIfAbsent(
              databaseName, database -> ConcurrentHashMap.newKeySet());
      processorInfos.add(tsFileProcessorInfo);
      metricService.createAutoGauge(
          Metric.MEM.toString(),
          MetricLevel.IMPORTANT,
          processorInfos,
          infos -> infos.stream().mapToLong(TsFileProcessorInfo::getMemCost).sum(),
          Tag.NAME.toString(),
          "chunkMetaData_" + databaseName);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    synchronized (DATABASE_TO_PROCESSOR_INFOS) {
      Set<TsFileProcessorInfo> processorInfos = DATABASE_TO_PROCESSOR_INFOS.get(databaseName);
      if (Objects.nonNull(processorInfos)) {
        processorInfos.remove(tsFileProcessorInfo);
        if (!processorInfos.isEmpty()) {
          return;
        }
        DATABASE_TO_PROCESSOR_INFOS.remove(databaseName);
      }
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.MEM.toString(),
          Tag.NAME.toString(),
          "chunkMetaData_" + databaseName);
    }
  }
}
