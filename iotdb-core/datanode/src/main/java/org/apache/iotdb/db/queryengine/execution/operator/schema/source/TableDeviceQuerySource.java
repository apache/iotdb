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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceFilterUtil;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.schemaengine.rescon.ISchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TableDeviceQuerySource implements ISchemaSource<IDeviceSchemaInfo> {

  private final String tableName;

  private final List<List<SchemaFilter>> idDeterminedPredicateList;

  private final List<ColumnHeader> columnHeaderList;
  private final List<TsTableColumnSchema> columnSchemaList;
  private final DevicePredicateFilter filter;
  private @Nonnull List<PartialPath> devicePatternList;

  public TableDeviceQuerySource(
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final List<ColumnHeader> columnHeaderList,
      final List<TsTableColumnSchema> columnSchemaList,
      final DevicePredicateFilter filter) {
    this.tableName = tableName;
    this.idDeterminedPredicateList = idDeterminedPredicateList;
    this.columnHeaderList = columnHeaderList;
    // Calculate this outside to save cpu
    this.columnSchemaList = columnSchemaList;
    this.filter = filter;
    this.devicePatternList = getDevicePatternList(database, tableName, idDeterminedPredicateList);
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(final ISchemaRegion schemaRegion) {
    return new ISchemaReader<IDeviceSchemaInfo>() {

      private ISchemaReader<IDeviceSchemaInfo> deviceReader;
      private Throwable throwable;
      private int index = 0;
      private IDeviceSchemaInfo next;

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
        if (Objects.nonNull(next)) {
          return true;
        }

        if (Objects.isNull(filter)) {
          if (innerHasNext()) {
            next = deviceReader.next();
            return true;
          }
          return false;
        }

        if (filter.hasNext()) {
          next = filter.next();
          return true;
        }

        while (innerHasNext() && !filter.hasNext()) {
          filter.addBatch(deviceReader.next());
        }

        if (!filter.hasNext()) {
          filter.prepareBatchResult();
        }

        if (filter.hasNext()) {
          next = filter.next();
          return true;
        }
        return false;
      }

      private boolean innerHasNext() {
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
            deviceReader = schemaRegion.getTableDeviceReader(devicePatternList.get(index));
            index++;
            if (deviceReader.hasNext()) {
              return true;
            } else {
              deviceReader.close();
            }
          }
          return false;
        } catch (final Exception e) {
          throw new SchemaExecutionException(e.getMessage(), e);
        }
      }

      @Override
      public IDeviceSchemaInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final IDeviceSchemaInfo result = next;
        next = null;
        return result;
      }

      @Override
      public void close() throws Exception {
        if (Objects.nonNull(deviceReader)) {
          deviceReader.close();
        }
        if (Objects.nonNull(filter)) {
          filter.close();
        }
      }
    };
  }

  public static @Nonnull List<PartialPath> getDevicePatternList(
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList) {
    if (Objects.isNull(DataNodeTableCache.getInstance().getTable(database, tableName))) {
      TableMetadataImpl.throwTableNotExistsException(database, tableName);
    }
    return DeviceFilterUtil.convertToDevicePattern(
        database,
        tableName,
        DataNodeTableCache.getInstance().getTable(database, tableName).getIdNums(),
        idDeterminedPredicateList);
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return columnHeaderList;
  }

  @Override
  public void transformToTsBlockColumns(
      final IDeviceSchemaInfo schemaInfo, final TsBlockBuilder builder, final String database) {
    transformToTsBlockColumns(schemaInfo, builder, columnSchemaList, 3);
  }

  public static void transformToTsBlockColumns(
      final IDeviceSchemaInfo schemaInfo,
      final TsBlockBuilder builder,
      final List<TsTableColumnSchema> columnSchemaList,
      int idIndex) {
    builder.getTimeColumnBuilder().writeLong(0L);
    int resultIndex = 0;
    final String[] pathNodes = schemaInfo.getRawNodes();
    for (final TsTableColumnSchema columnSchema : columnSchemaList) {
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
        if (pathNodes.length <= idIndex || pathNodes[idIndex] == null) {
          builder.getColumnBuilder(resultIndex).appendNull();
        } else {
          builder
              .getColumnBuilder(resultIndex)
              .writeBinary(new Binary(pathNodes[idIndex], TSFileConfig.STRING_CHARSET));
        }
        idIndex++;
      } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
        final Binary attributeValue = schemaInfo.getAttributeValue(columnSchema.getColumnName());
        if (attributeValue == null) {
          builder.getColumnBuilder(resultIndex).appendNull();
        } else {
          builder.getColumnBuilder(resultIndex).writeBinary(attributeValue);
        }
      }
      resultIndex++;
    }
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(final ISchemaRegion schemaRegion) {
    return (Objects.isNull(idDeterminedPredicateList)
            || idDeterminedPredicateList.isEmpty()
            || idDeterminedPredicateList.stream().allMatch(List::isEmpty))
        && Objects.isNull(filter);
  }

  @Override
  public long getSchemaStatistic(final ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getTableDevicesNumber(tableName);
  }

  @Override
  public long getMaxMemory(final ISchemaRegion schemaRegion) {
    final ISchemaRegionStatistics statistics = schemaRegion.getSchemaRegionStatistics();
    final long devicesNumber = statistics.getTableDevicesNumber(tableName);
    return devicePatternList.stream().allMatch(path -> ((ExtendedPartialPath) path).isNormalPath())
        ? Math.min(
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
            devicePatternList.stream()
                    .map(
                        devicePattern ->
                            Arrays.stream(
                                    devicePattern.getNodes(), 3, devicePattern.getNodeLength())
                                .map(RamUsageEstimator::sizeOf)
                                .reduce(0L, Long::sum))
                    .reduce(0L, Long::sum)
                + (devicesNumber > 0
                    ? devicePatternList.size()
                        * statistics.getTableAttributeMemory(tableName)
                        / devicesNumber
                    : 0))
        : TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }
}
