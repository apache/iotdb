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

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceFilterUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowTableDevicesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.NoSuchElementException;

public class TableDeviceSchemaSource implements ISchemaSource<IDeviceSchemaInfo> {

  private String database;

  private String tableName;

  private List<List<SchemaFilter>> idDeterminedFilterList;

  private SchemaFilter idFuzzyFilter;

  private List<ColumnHeader> columnHeaderList;

  public TableDeviceSchemaSource(
      String database,
      String tableName,
      List<List<SchemaFilter>> idDeterminedFilterList,
      SchemaFilter idFuzzyFilter,
      List<ColumnHeader> columnHeaderList) {
    this.database = database;
    this.tableName = tableName;
    this.idDeterminedFilterList = idDeterminedFilterList;
    this.idFuzzyFilter = idFuzzyFilter;
    this.columnHeaderList = columnHeaderList;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    List<PartialPath> devicePatternList = getDevicePatternList();
    return new ISchemaReader<IDeviceSchemaInfo>() {

      private ISchemaReader<IDeviceSchemaInfo> deviceReader;
      private Throwable throwable;
      private int index = 0;

      @Override
      public boolean isSuccess() {
        return throwable == null && (deviceReader == null || deviceReader.isSuccess());
      }

      @Override
      public Throwable getFailure() {
        if (throwable != null) {
          return throwable;
        } else if (deviceReader != null) {
          return deviceReader.getFailure();
        }
        return null;
      }

      @Override
      public ListenableFuture<?> isBlocked() {
        return NOT_BLOCKED;
      }

      @Override
      public boolean hasNext() {
        try {
          if (throwable != null) {
            return false;
          }
          if (deviceReader != null) {
            if (deviceReader.hasNext()) {
              return true;
            } else {
              deviceReader.close();
              if (!deviceReader.isSuccess()) {
                throwable = deviceReader.getFailure();
                return false;
              }
            }
          }

          while (index < devicePatternList.size()) {
            deviceReader =
                schemaRegion.getTableDeviceReader(
                    new ShowTableDevicesPlan(devicePatternList.get(index), idFuzzyFilter));
            index++;
            if (deviceReader.hasNext()) {
              return true;
            } else {
              deviceReader.close();
            }
          }
          return false;
        } catch (Exception e) {
          throw new SchemaExecutionException(e.getMessage(), e);
        }
      }

      @Override
      public IDeviceSchemaInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return deviceReader.next();
      }

      @Override
      public void close() throws Exception {
        if (deviceReader != null) {
          deviceReader.close();
        }
      }
    };
  }

  private List<PartialPath> getDevicePatternList() {
    return DeviceFilterUtil.convertToDevicePattern(
        database,
        DataNodeTableCache.getInstance().getTable(database, tableName),
        idDeterminedFilterList);
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return columnHeaderList;
  }

  @Override
  public void transformToTsBlockColumns(
      IDeviceSchemaInfo schemaInfo, TsBlockBuilder builder, String database) {
    builder.getTimeColumnBuilder().writeLong(0L);
    int resultIndex = 0;
    int idIndex = 0;
    String[] pathNodes = schemaInfo.getRawNodes();
    TsTable table = DataNodeTableCache.getInstance().getTable(this.database, tableName);
    TsTableColumnSchema columnSchema;
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnSchema = table.getColumnSchema(columnHeader.getColumnName());
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        if (pathNodes[idIndex + 3] == null) {
          builder.getColumnBuilder(resultIndex).appendNull();
        } else {
          builder
              .getColumnBuilder(resultIndex)
              .writeBinary(new Binary(pathNodes[idIndex + 3], TSFileConfig.STRING_CHARSET));
        }
        idIndex++;
      } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
        String attributeValue = schemaInfo.getAttributeValue(columnHeader.getColumnName());
        if (attributeValue == null) {
          builder.getColumnBuilder(resultIndex).appendNull();
        } else {
          builder
              .getColumnBuilder(resultIndex)
              .writeBinary(
                  new Binary(
                      schemaInfo.getAttributeValue(columnHeader.getColumnName()),
                      TSFileConfig.STRING_CHARSET));
        }
      }
      resultIndex++;
    }
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return false;
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return 0;
  }
}
