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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public class TableAggregationTableScanOperator extends TableScanOperator {

  List<Aggregator> aggregators;

  public TableAggregationTableScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      int measurementColumnCount,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      int maxTsBlockLineNum) {
    super(
        context,
        sourceId,
        columnSchemas,
        columnsIndexArray,
        measurementColumnCount,
        deviceEntries,
        scanOrder,
        seriesScanOptions,
        measurementColumnNames,
        measurementSchemas,
        maxTsBlockLineNum);
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished();
  }

  @Override
  public boolean isFinished() throws Exception {
    return (retainedTsBlock == null);
    // && (currentDeviceIndex >= deviceCount || seriesScanOptions.limitConsumedUp());
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
