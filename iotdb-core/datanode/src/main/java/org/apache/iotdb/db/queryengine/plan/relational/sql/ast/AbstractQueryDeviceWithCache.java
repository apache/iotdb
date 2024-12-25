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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class AbstractQueryDeviceWithCache extends AbstractTraverseDevice {

  // For query devices fully in cache
  protected List<ShowDevicesResult> results = new ArrayList<>();

  protected AbstractQueryDeviceWithCache(
      final NodeLocation location, final Table table, final Expression rawExpression) {
    super(location, table, rawExpression);
  }

  protected AbstractQueryDeviceWithCache(final String database, final String tableName) {
    super(database, tableName);
  }

  public boolean parseRawExpression(
      final TsTable tableInstance,
      final List<String> attributeColumns,
      final MPPQueryContext context) {
    if (Objects.isNull(where)) {
      return true;
    }
    final List<DeviceEntry> entries = new ArrayList<>();
    final boolean needFetch =
        super.parseRawExpression(entries, tableInstance, attributeColumns, context);
    if (!needFetch) {
      results =
          entries.stream()
              .map(
                  deviceEntry ->
                      ShowDevicesResult.convertDeviceEntry2ShowDeviceResult(
                          deviceEntry, attributeColumns))
              .collect(Collectors.toList());
    }
    return needFetch;
  }

  public static List<ColumnHeader> getDeviceColumnHeaderList(
      final String database, final String tableName) {
    return DataNodeTableCache.getInstance().getTable(database, tableName).getColumnList().stream()
        .filter(
            columnSchema ->
                columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)
                    || columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE))
        .map(
            columnSchema ->
                new ColumnHeader(columnSchema.getColumnName(), columnSchema.getDataType()))
        .collect(Collectors.toList());
  }

  public abstract DatasetHeader getDataSetHeader();

  public abstract TsBlock getTsBlock(final Analysis analysis);
}
