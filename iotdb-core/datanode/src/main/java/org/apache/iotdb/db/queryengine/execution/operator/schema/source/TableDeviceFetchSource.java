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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.Objects;

public class TableDeviceFetchSource implements ISchemaSource<IDeviceSchemaInfo> {

  private final String database;

  private final String tableName;

  private final List<Object[]> deviceIdList;

  private final List<ColumnHeader> columnHeaderList;

  public TableDeviceFetchSource(
      final String database,
      final String tableName,
      final List<Object[]> deviceIdList,
      final List<ColumnHeader> columnHeaderList) {
    this.database = database;
    this.tableName = tableName;
    this.deviceIdList = deviceIdList;
    this.columnHeaderList = columnHeaderList;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(final ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getTableDeviceReader(tableName, deviceIdList);
    } catch (final MetadataException e) {
      throw new SchemaExecutionException(e);
    }
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return columnHeaderList;
  }

  @Override
  public void transformToTsBlockColumns(
      final IDeviceSchemaInfo schemaInfo, final TsBlockBuilder builder, final String database) {
    builder.getTimeColumnBuilder().writeLong(0L);
    int resultIndex = 0;
    int idIndex = 0;
    final String[] pathNodes = schemaInfo.getRawNodes();
    final TsTable table = DataNodeTableCache.getInstance().getTable(this.database, tableName);
    TsTableColumnSchema columnSchema;
    for (final ColumnHeader columnHeader : columnHeaderList) {
      columnSchema = table.getColumnSchema(columnHeader.getColumnName());
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        if (pathNodes.length <= idIndex + 3 || pathNodes[idIndex + 3] == null) {
          builder.getColumnBuilder(resultIndex).appendNull();
        } else {
          builder
              .getColumnBuilder(resultIndex)
              .writeBinary(new Binary(pathNodes[idIndex + 3], TSFileConfig.STRING_CHARSET));
        }
        idIndex++;
      } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
        if (Objects.isNull(schemaInfo.getAttributeValue(columnHeader.getColumnName()))) {
          builder.getColumnBuilder(resultIndex).appendNull();
        } else {
          builder
              .getColumnBuilder(resultIndex)
              .writeBinary(schemaInfo.getAttributeValue(columnHeader.getColumnName()));
        }
      }
      resultIndex++;
    }
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(final ISchemaRegion schemaRegion) {
    return false;
  }

  @Override
  public long getSchemaStatistic(final ISchemaRegion schemaRegion) {
    return 0;
  }
}
