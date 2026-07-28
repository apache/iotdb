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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Projects source-table tablets into writable-view tablets for table-model subscriptions. */
public class WritableViewTabletProjector {

  private final String databaseName;
  private final WritableView writableView;
  private final Map<String, List<TsTableColumnSchema>> sourceColumnNameToViewColumns;

  public WritableViewTabletProjector(final String databaseName, final WritableView writableView) {
    this.databaseName = databaseName;
    this.writableView = writableView;
    this.sourceColumnNameToViewColumns = new HashMap<>();
    for (final TsTableColumnSchema viewColumn : writableView.getColumnList()) {
      if (Objects.isNull(viewColumn)
          || viewColumn.getColumnCategory() == TsTableColumnCategory.TIME) {
        continue;
      }
      final String sourceColumnName =
          writableView.getOriginalColumnName(viewColumn.getColumnName());
      if (Objects.nonNull(sourceColumnName)) {
        sourceColumnNameToViewColumns
            .computeIfAbsent(sourceColumnName, key -> new ArrayList<>())
            .add(viewColumn);
      }
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getSourceDatabaseName() {
    return writableView.getSourceTableDatabase();
  }

  public String getSourceTableName() {
    return writableView.getSourceTableName();
  }

  public Tablet project(final String sourceDatabaseName, final Tablet sourceTablet) {
    if (Objects.isNull(sourceTablet)
        || !Objects.equals(getSourceDatabaseName(), sourceDatabaseName)
        || !Objects.equals(getSourceTableName(), sourceTablet.getTableName())
        || Objects.isNull(sourceTablet.getSchemas())
        || Objects.isNull(sourceTablet.getValues())) {
      return null;
    }

    final List<IMeasurementSchema> projectedSchemas = new ArrayList<>();
    final List<ColumnCategory> projectedCategories = new ArrayList<>();
    final List<Object> projectedValues = new ArrayList<>();
    final List<BitMap> projectedBitMaps = new ArrayList<>();
    final BitMap[] sourceBitMaps = sourceTablet.getBitMaps();

    final List<IMeasurementSchema> sourceSchemas = sourceTablet.getSchemas();
    for (int sourceColumnIndex = 0;
        sourceColumnIndex < sourceSchemas.size()
            && sourceColumnIndex < sourceTablet.getValues().length;
        sourceColumnIndex++) {
      final IMeasurementSchema sourceSchema = sourceSchemas.get(sourceColumnIndex);
      if (Objects.isNull(sourceSchema)) {
        continue;
      }
      final List<TsTableColumnSchema> viewColumns =
          sourceColumnNameToViewColumns.get(sourceSchema.getMeasurementName());
      if (Objects.isNull(viewColumns)) {
        continue;
      }
      for (final TsTableColumnSchema viewColumn : viewColumns) {
        projectedSchemas.add(
            new MeasurementSchema(viewColumn.getColumnName(), viewColumn.getDataType()));
        projectedCategories.add(toTsFileColumnCategory(viewColumn.getColumnCategory()));
        projectedValues.add(sourceTablet.getValues()[sourceColumnIndex]);
        projectedBitMaps.add(
            Objects.nonNull(sourceBitMaps) && sourceColumnIndex < sourceBitMaps.length
                ? sourceBitMaps[sourceColumnIndex]
                : null);
      }
    }

    if (projectedSchemas.isEmpty()) {
      return null;
    }

    return new Tablet(
        writableView.getTableName(),
        projectedSchemas,
        projectedCategories,
        sourceTablet.getTimestamps(),
        projectedValues.toArray(new Object[0]),
        projectedBitMaps.stream().anyMatch(Objects::nonNull)
            ? projectedBitMaps.toArray(new BitMap[0])
            : null,
        sourceTablet.getRowSize());
  }

  private static ColumnCategory toTsFileColumnCategory(final TsTableColumnCategory columnCategory) {
    switch (columnCategory) {
      case TAG:
        return ColumnCategory.TAG;
      case FIELD:
        return ColumnCategory.FIELD;
      case ATTRIBUTE:
        return ColumnCategory.ATTRIBUTE;
      default:
        throw new IllegalArgumentException(String.valueOf(columnCategory));
    }
  }
}
