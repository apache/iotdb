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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeAlignedDeviceViewScanOperator.getNthIdColumnValueForTree;

public class TreeAlignedDeviceViewAggregationScanOperator
    extends AbstractAggregationTableScanOperator {

  // in iotdb, db level should at least be 2 level, like root.db
  // if db level is 2, idColumnStartIndex is 0, and we use should treeDBLength to extract the first
  // id column value
  // if db level is larger than 2, idColumnStartIndex will be db level - 2
  private final int idColumnStartIndex;

  // only take effect, if db level is 2 level, for root.db.d1, IDeviceId will be [root.db.d1],
  // treeDBLength will be 7 (root.db)
  private final int treeDBLength;

  public TreeAlignedDeviceViewAggregationScanOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      List<ColumnSchema> aggColumnSchemas,
      int[] aggColumnsIndexArray,
      List<DeviceEntry> deviceEntries,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      Set<String> allSensors,
      List<IMeasurementSchema> measurementSchemas,
      List<TableAggregator> tableAggregators,
      List<ColumnSchema> groupingKeySchemas,
      int[] groupingKeyIndex,
      ITableTimeRangeIterator tableTimeRangeIterator,
      boolean ascending,
      boolean canUseStatistics,
      List<Integer> aggregatorInputChannels,
      int idColumnStartIndex,
      int treeDBLength) {
    super(
        sourceId,
        context,
        aggColumnSchemas,
        aggColumnsIndexArray,
        deviceEntries,
        seriesScanOptions,
        measurementColumnNames,
        allSensors,
        measurementSchemas,
        tableAggregators,
        groupingKeySchemas,
        groupingKeyIndex,
        tableTimeRangeIterator,
        ascending,
        canUseStatistics,
        aggregatorInputChannels);
    this.idColumnStartIndex = idColumnStartIndex;
    this.treeDBLength = treeDBLength;
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return getNthIdColumnValueForTree(
        deviceEntry, idColumnIndex, this.idColumnStartIndex, this.treeDBLength);
  }
}
