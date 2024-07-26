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
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StatementTestUtils {

  public static final String TEST_PARTITION_EXECUTOR =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";
  public static final int TEST_SERIES_SLOT_NUM = 1000;

  private StatementTestUtils() {
    // util class
  }

  public static String tableName() {
    return "table1";
  }

  public static String[] genColumnNames() {
    return new String[] {"id1", "attr1", "m1"};
  }

  public static TSDataType[] genDataTypes() {
    return new TSDataType[] {TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE};
  }

  public static MeasurementSchema[] genMeasurementSchemas() {
    return new MeasurementSchema[] {
      new MeasurementSchema("id1", TSDataType.STRING),
      new MeasurementSchema("attr1", TSDataType.STRING),
      new MeasurementSchema("m1", TSDataType.DOUBLE)
    };
  }

  public static TsTableColumnCategory[] genColumnCategories() {
    return new TsTableColumnCategory[] {
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
    return genColumns(3, 0);
  }

  public static Object[] genColumns(int cnt, int offset) {
    final Binary[] ids = new Binary[cnt];
    final Binary[] attrs = new Binary[cnt];
    final double[] values = new double[cnt];
    for (int i = 0; i < cnt; i++) {
      ids[i] = new Binary(("id:" + (i + offset)).getBytes(StandardCharsets.UTF_8));
      attrs[i] = new Binary(("attr:" + (i + offset)).getBytes(StandardCharsets.UTF_8));
      values[i] = (i + offset) * 1.0;
    }

    return new Object[] {ids, attrs, values};
  }

  public static Object[] genValues(int offset) {
    return new Object[] {
      new Binary(("id:" + offset).getBytes(StandardCharsets.UTF_8)),
      new Binary(("attr:" + offset).getBytes(StandardCharsets.UTF_8)),
      offset * 1.0
    };
  }

  public static long[] genTimestamps() {
    return genTimestamps(3, 0);
  }

  public static long[] genTimestamps(int cnt, int offset) {
    final long[] timestamps = new long[cnt];
    for (int i = 0; i < cnt; i++) {
      timestamps[i] = i + offset;
    }
    return timestamps;
  }

  public static InsertRowStatement genInsertRowStatement(boolean writeToTable, int offset) {
    String[] measurements = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();

    Object[] values = genValues(offset);
    long[] timestamps = genTimestamps(1, offset);

    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(new PartialPath(new String[] {tableName()}));
    insertStatement.setMeasurements(measurements);
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setColumnCategories(columnCategories);
    insertStatement.setValues(values);
    insertStatement.setTime(timestamps[0]);
    insertStatement.setWriteToTable(writeToTable);

    return insertStatement;
  }

  public static InsertTabletStatement genInsertTabletStatement(
      boolean writeToTable, int rowCnt, int offset) {
    String[] measurements = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();

    Object[] columns = genColumns(rowCnt, offset);
    long[] timestamps = genTimestamps(rowCnt, offset);

    InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
    insertTabletStatement.setDevicePath(new PartialPath(new String[] {tableName()}));
    insertTabletStatement.setMeasurements(measurements);
    insertTabletStatement.setDataTypes(dataTypes);
    insertTabletStatement.setColumnCategories(columnCategories);
    insertTabletStatement.setColumns(columns);
    insertTabletStatement.setTimes(timestamps);
    insertTabletStatement.setWriteToTable(writeToTable);
    insertTabletStatement.setRowCount(timestamps.length);

    return insertTabletStatement;
  }

  public static RelationalInsertTabletNode genInsertTabletNode(int rowCnt, int offset) {
    String[] measurements = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();
    final MeasurementSchema[] measurementSchemas = genMeasurementSchemas();

    Object[] columns = genColumns(rowCnt, offset);
    long[] timestamps = genTimestamps(rowCnt, offset);

    return new RelationalInsertTabletNode(
        new PlanNodeId(offset + "-" + rowCnt),
        new PartialPath(new String[] {tableName()}),
        true,
        measurements,
        dataTypes,
        measurementSchemas,
        timestamps,
        null,
        columns,
        rowCnt,
        columnCategories);
  }

  public static RelationalInsertRowNode genInsertRowNode(int offset) {
    String[] measurements = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();

    Object[] values = genValues(offset);
    long timestamp = genTimestamps(1, offset)[0];

    RelationalInsertRowNode relationalInsertRowNode =
        new RelationalInsertRowNode(
            new PlanNodeId(offset + "-" + 1),
            new PartialPath(new String[] {tableName()}),
            true,
            measurements,
            dataTypes,
            timestamp,
            values,
            false,
            columnCategories);
    relationalInsertRowNode.setMeasurementSchemas(genMeasurementSchemas());
    return relationalInsertRowNode;
  }

  public static InsertTabletStatement genInsertTabletStatement(boolean writeToTable) {
    return genInsertTabletStatement(writeToTable, 3, 0);
  }

  public static InsertRowStatement genInsertRowStatement(boolean writeToTable) {
    return genInsertRowStatement(writeToTable, 0);
  }

  public static TsTable genTsTable() {
    final TsTable tsTable = new TsTable(tableName());
    String[] measurements = genColumnNames();
    TSDataType[] dataTypes = genDataTypes();
    TsTableColumnCategory[] columnCategories = genColumnCategories();
    for (int i = 0; i < columnCategories.length; i++) {
      switch (columnCategories[i]) {
        case ID:
          tsTable.addColumnSchema(new IdColumnSchema(measurements[i], dataTypes[i]));
          break;
        case ATTRIBUTE:
          tsTable.addColumnSchema(new AttributeColumnSchema(measurements[i], dataTypes[i]));
          break;
        case MEASUREMENT:
        default:
          tsTable.addColumnSchema(
              new MeasurementColumnSchema(
                  measurements[i], dataTypes[i], TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
          break;
      }
    }
    return tsTable;
  }
}
