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

package org.apache.iotdb.db.queryengine.plan.statement;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.List;
import org.apache.tsfile.utils.Binary;

public class StatementTestUtils {

  private StatementTestUtils() {
    // util class
  }

  public static String tableName() {
    return "table1";
  }

  public static String[] genColumnNames() {
    return new String[]{"id1", "attr1", "m1"};
  }

  public static TSDataType[] genDataTypes() {
    return new TSDataType[]{TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE};
  }

  public static TsTableColumnCategory[] genColumnCategories() {
    return new TsTableColumnCategory[]{
        TsTableColumnCategory.ID, TsTableColumnCategory.ATTRIBUTE, TsTableColumnCategory.MEASUREMENT
    };
  }

  public static List<ColumnSchema> genColumnSchema() {
    String[] columnNames = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();

    List<ColumnSchema> result = new ArrayList<>();
    for (int i = 0; i < columnNames.length; i++) {
      result.add(
          new ColumnSchema(
              columnNames[i], TypeFactory.getType(dataTypes[i]), false, columnCategories[i]));
    }
    return result;
  }

  public static TableSchema genTableSchema() {
    return new TableSchema(tableName(), genColumnSchema());
  }

  public static Object[] genColumns() {
    return new Object[]{
        new String[]{"a", "b", "c"},
        new String[]{"x", "y", "z"},
        new double[]{1.0, 2.0, 3.0}
    };
  }

  public static long[] genTimestamps() {
    return new long[]{1L, 2L, 3L};
  }

  public static InsertTabletStatement genInsertTabletStatement(boolean writeToTable) {
    String[] measurements = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();

    Object[] columns = genColumns();
    long[] timestamps = genTimestamps();

    InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
    insertTabletStatement.setDevicePath(new PartialPath(new String[]{tableName()}));
    insertTabletStatement.setMeasurements(measurements);
    insertTabletStatement.setDataTypes(dataTypes);
    insertTabletStatement.setColumnCategories(columnCategories);
    insertTabletStatement.setColumns(columns);
    insertTabletStatement.setTimes(timestamps);
    insertTabletStatement.setWriteToTable(writeToTable);
    insertTabletStatement.setRowCount(timestamps.length);

    return insertTabletStatement;
  }
}
