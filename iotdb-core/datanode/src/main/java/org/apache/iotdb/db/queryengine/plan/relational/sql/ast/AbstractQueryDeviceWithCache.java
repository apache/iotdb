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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowDevicesResult;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class AbstractQueryDeviceWithCache extends AbstractTraverseDevice {

  // For query devices fully in cache
  protected List<ShowDevicesResult> results = new ArrayList<>();

  protected List<ColumnHeader> columnHeaderList;

  protected AbstractQueryDeviceWithCache(final String tableName, final Expression rawExpression) {
    super(tableName, rawExpression);
  }

  public boolean parseRawExpression(
      final TsTable tableInstance,
      final List<String> attributeColumns,
      final MPPQueryContext context) {
    if (Objects.isNull(rawExpression)) {
      return true;
    }
    final List<DeviceEntry> entries = new ArrayList<>();
    final boolean needFetch =
        super.parseRawExpression(entries, tableInstance, attributeColumns, context);
    if (needFetch) {
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

  public List<ColumnHeader> getColumnHeaderList() {
    return columnHeaderList;
  }

  public abstract void setColumnHeaderList();

  public DatasetHeader getDataSetHeader() {
    return new DatasetHeader(columnHeaderList, true);
  }

  public TsBlock getTsBlock() {
    final TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(
            columnHeaderList.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList()));
    buildTsBlock(tsBlockBuilder);
    return tsBlockBuilder.build();
  }

  protected abstract void buildTsBlock(final TsBlockBuilder tsBlockBuilder);
}
