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

package org.apache.iotdb.confignode.persistence.metric;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.persistence.partition.StorageGroupPartitionTable;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class StorageGroupPartitionTableMetrics implements IMetricSet {
  private StorageGroupPartitionTable storageGroupPartitionTable;

  public StorageGroupPartitionTableMetrics(StorageGroupPartitionTable storageGroupPartitionTable) {
    this.storageGroupPartitionTable = storageGroupPartitionTable;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.getOrCreateAutoGauge(
        Metric.REGION.toString(),
        MetricLevel.NORMAL,
        storageGroupPartitionTable,
        o -> o.getRegionGroupCount(TConsensusGroupType.SchemaRegion),
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.getOrCreateAutoGauge(
        Metric.REGION.toString(),
        MetricLevel.NORMAL,
        storageGroupPartitionTable,
        o -> o.getRegionGroupCount(TConsensusGroupType.DataRegion),
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
    // TODO slot will be updated in the future
    metricService.getOrCreateAutoGauge(
        Metric.SLOT.toString(),
        MetricLevel.NORMAL,
        storageGroupPartitionTable,
        StorageGroupPartitionTable::getSchemaPartitionMapSize,
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        "schemaSlotNumber");
    metricService.getOrCreateAutoGauge(
        Metric.SLOT.toString(),
        MetricLevel.NORMAL,
        storageGroupPartitionTable,
        StorageGroupPartitionTable::getDataPartitionMapSize,
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        "dataSlotNumber");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.REGION.toString(),
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        TConsensusGroupType.SchemaRegion.toString());
    metricService.remove(
        MetricType.GAUGE,
        Metric.REGION.toString(),
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        TConsensusGroupType.DataRegion.toString());
    // TODO slot will be updated in the future
    metricService.remove(
        MetricType.GAUGE,
        Metric.SLOT.toString(),
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        "schemaSlotNumber");
    metricService.remove(
        MetricType.GAUGE,
        Metric.SLOT.toString(),
        Tag.NAME.toString(),
        storageGroupPartitionTable.getStorageGroupName(),
        Tag.TYPE.toString(),
        "dataSlotNumber");
  }
}
