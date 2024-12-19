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

import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Set;

public class TreeAlignedDeviceViewScanOperator extends AbstractTableScanOperator {

  // in iotdb, db level should at least be 2 level, like root.db
  // if db level is 2, idColumnStartIndex is 0, and we use should treeDBLength to extract the first
  // id column value
  // if db level is larger than 2, idColumnStartIndex will be db level - 2
  private final int idColumnStartIndex;

  // only take effect, if db level is 2 level, for root.db.d1, IDeviceId will be [root.db.d1],
  // treeDBLength will be 7 (root.db)
  private final int treeDBLength;

  public TreeAlignedDeviceViewScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      List<ColumnSchema> columnSchemas,
      int[] columnsIndexArray,
      List<DeviceEntry> deviceEntries,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      List<String> measurementColumnNames,
      Set<String> allSensors,
      List<IMeasurementSchema> measurementSchemas,
      int idColumnStartIndex,
      int treeDBLength,
      int maxTsBlockLineNum) {
    super(
        context,
        sourceId,
        columnSchemas,
        columnsIndexArray,
        deviceEntries,
        scanOrder,
        seriesScanOptions,
        measurementColumnNames,
        allSensors,
        measurementSchemas,
        maxTsBlockLineNum);
    this.idColumnStartIndex = idColumnStartIndex;
    this.treeDBLength = treeDBLength;
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return getNthIdColumnValueForTree(
        deviceEntry, idColumnIndex, this.idColumnStartIndex, this.treeDBLength);
  }

  /**
   * getNthIdColumnValueForTree
   *
   * @param idColumnStartIndex in iotdb, db level should at least be 2 level, like root.db, if db
   *     level is 2, idColumnStartIndex is 0, and we use should treeDBLength to extract the first id
   *     column value. if db level is larger than 2, idColumnStartIndex will be db level - 2
   * @param treeDBLength only take effect, if db level is 2 level, for root.db.d1, IDeviceId will be
   *     [root.db.d1], treeDBLength will be 7 (root.db)
   */
  public static String getNthIdColumnValueForTree(
      DeviceEntry deviceEntry, int idColumnIndex, int idColumnStartIndex, int treeDBLength) {
    if (idColumnStartIndex == 0) {
      if (idColumnIndex == 0) {
        // + 1 for skipping the `.` after db name, for root.db.d1, IDeviceId will be [root.db.d1],
        // treeDBLength will be 7 (root.db), we only need the `d1` which is starting from 8
        return ((String) deviceEntry.getNthSegment(0)).substring(treeDBLength + 1);
      } else {
        return ((String) deviceEntry.getNthSegment(idColumnIndex));
      }
    } else {
      // + idColumnStartIndex for skipping the tree db name segment
      return ((String) deviceEntry.getNthSegment(idColumnIndex + idColumnStartIndex));
    }
  }
}
