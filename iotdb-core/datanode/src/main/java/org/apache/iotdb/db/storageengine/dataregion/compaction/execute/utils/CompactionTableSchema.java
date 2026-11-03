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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionTableSchemaNotMatchException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class CompactionTableSchema extends TableSchema {
  public CompactionTableSchema(String tableName) {
    super(tableName);
  }

  public boolean merge(TableSchema tableSchema) {
    if (tableSchema == null) {
      return true;
    }
    if (!tableSchema.getTableName().equals(this.tableName)) {
      throw new CompactionTableSchemaNotMatchException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_THIS_TABLENAME_IS_S_MERGE_TABLENAME_IS_S_4B05FA97,
              tableName,
              tableSchema.getTableName()));
    }

    final List<IMeasurementSchema> otherSchemaColumnSchemas = tableSchema.getColumnSchemas();
    final List<ColumnCategory> otherSchemaColumnCategories = tableSchema.getColumnTypes();
    final List<IMeasurementSchema> idColumnSchemasToMerge = new ArrayList<>();
    IMeasurementSchema timeColumnToMerge = null;

    for (int i = 0; i < otherSchemaColumnCategories.size(); i++) {
      final ColumnCategory columnCategory = otherSchemaColumnCategories.get(i);
      if (columnCategory == ColumnCategory.TIME) {
        // A TableSchema must contain at most one TIME column.  Keep it so that a
        // time-only table remains recognizable as a table-model TsFile.
        if (timeColumnToMerge != null) {
          return false;
        }
        timeColumnToMerge = otherSchemaColumnSchemas.get(i);
      } else if (columnCategory == ColumnCategory.TAG) {
        idColumnSchemasToMerge.add(otherSchemaColumnSchemas.get(i));
      }
    }

    if (timeColumnToMerge != null) {
      final int existingTimeColumnIndex = findColumnIndex(ColumnCategory.TIME);
      if (existingTimeColumnIndex < 0) {
        // Keep TIME before the id columns, matching the schema order produced by
        // TsFileTableSchemaUtil.
        measurementSchemas.add(0, timeColumnToMerge);
        columnCategories.add(0, ColumnCategory.TIME);
      } else if (!isSameTimeColumn(
          measurementSchemas.get(existingTimeColumnIndex), timeColumnToMerge)) {
        return false;
      }
    }

    // Only TAG columns are id columns.  Filter them out from the target schema so TIME
    // (and any FIELD already appended by update()) does not participate in the prefix check.
    final List<IMeasurementSchema> currentIdColumns = new ArrayList<>();
    for (int i = 0; i < columnCategories.size(); i++) {
      if (columnCategories.get(i) == ColumnCategory.TAG) {
        currentIdColumns.add(measurementSchemas.get(i));
      }
    }

    // check id column prefix
    final int prefixLength = Math.min(currentIdColumns.size(), idColumnSchemasToMerge.size());
    for (int i = 0; i < prefixLength; i++) {
      final IMeasurementSchema idColumnToMerge = idColumnSchemasToMerge.get(i);
      final IMeasurementSchema currentIdColumn = currentIdColumns.get(i);
      if (!idColumnToMerge.getMeasurementName().equals(currentIdColumn.getMeasurementName())) {
        return false;
      }
    }

    // add new id columns
    final List<IMeasurementSchema> newIdColumns =
        idColumnSchemasToMerge.subList(prefixLength, idColumnSchemasToMerge.size());
    for (IMeasurementSchema newIdColumn : newIdColumns) {
      columnCategories.add(ColumnCategory.TAG);
      measurementSchemas.add(newIdColumn);
    }
    return true;
  }

  private int findColumnIndex(final ColumnCategory columnCategory) {
    for (int i = 0; i < columnCategories.size(); i++) {
      if (columnCategories.get(i) == columnCategory) {
        return i;
      }
    }
    return -1;
  }

  private boolean isSameTimeColumn(
      final IMeasurementSchema current, final IMeasurementSchema incoming) {
    return current.getMeasurementName().equalsIgnoreCase(incoming.getMeasurementName())
        && current.getType() == incoming.getType();
  }

  public CompactionTableSchema copy() {
    CompactionTableSchema tableSchema = new CompactionTableSchema(this.tableName);
    tableSchema.measurementSchemas = new ArrayList<>(this.measurementSchemas);
    tableSchema.columnCategories = new ArrayList<>(this.columnCategories);
    return tableSchema;
  }
}
