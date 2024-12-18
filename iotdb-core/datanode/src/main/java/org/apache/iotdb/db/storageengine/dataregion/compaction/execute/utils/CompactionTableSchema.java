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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionTableSchemaNotMatchException;

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class CompactionTableSchema extends TableSchema {
  public CompactionTableSchema(String tableName) {
    super(tableName);
  }

  public void merge(TableSchema tableSchema) {
    if (tableSchema == null) {
      return;
    }
    if (!tableSchema.getTableName().equals(this.tableName)) {
      throw new CompactionTableSchemaNotMatchException(
          "this.tableName is " + tableName + " merge tableName is " + tableSchema.getTableName());
    }
    // filter id columns
    List<IMeasurementSchema> otherSchemaColumnSchemas = tableSchema.getColumnSchemas();
    List<ColumnCategory> otherSchemaColumnCategories = tableSchema.getColumnTypes();
    List<IMeasurementSchema> idColumnSchemasToMerge = new ArrayList<>();

    for (int i = 0; i < otherSchemaColumnCategories.size(); i++) {
      ColumnCategory columnCategory = otherSchemaColumnCategories.get(i);
      if (columnCategory != ColumnCategory.ID) {
        continue;
      }
      idColumnSchemasToMerge.add(otherSchemaColumnSchemas.get(i));
    }

    // check id column prefix
    int prefixLength = Math.min(this.columnCategories.size(), idColumnSchemasToMerge.size());
    for (int i = 0; i < prefixLength; i++) {
      IMeasurementSchema idColumnToMerge = idColumnSchemasToMerge.get(i);
      IMeasurementSchema currentIdColumn = measurementSchemas.get(i);
      if (!idColumnToMerge.getMeasurementName().equals(currentIdColumn.getMeasurementName())) {
        throw new CompactionTableSchemaNotMatchException(
            "current id column name is "
                + currentIdColumn.getMeasurementName()
                + ", other id column name in same position is "
                + idColumnToMerge.getMeasurementName());
      }
    }

    // add new id columns
    List<IMeasurementSchema> newIdColumns =
        idColumnSchemasToMerge.subList(prefixLength, idColumnSchemasToMerge.size());
    for (IMeasurementSchema newIdColumn : newIdColumns) {
      columnCategories.add(ColumnCategory.ID);
      measurementSchemas.add(newIdColumn);
    }
  }

  public CompactionTableSchema copy() {
    CompactionTableSchema tableSchema = new CompactionTableSchema(this.tableName);
    tableSchema.measurementSchemas = new ArrayList<>(this.measurementSchemas);
    tableSchema.columnCategories = new ArrayList<>(this.columnCategories);
    return tableSchema;
  }
}
