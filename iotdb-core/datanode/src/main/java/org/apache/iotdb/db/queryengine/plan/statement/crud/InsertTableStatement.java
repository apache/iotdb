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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.Insert;
import org.apache.iotdb.db.relational.sql.tree.LongLiteral;
import org.apache.iotdb.db.relational.sql.tree.Row;
import org.apache.iotdb.db.relational.sql.tree.Values;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class InsertTableStatement extends Statement implements ITableDeviceSchemaValidation {

  private final String database;

  private final String table;

  private List<String[]> deviceIdList;

  private List<String> attributeNameList;

  private List<List<String>> attributeValueList;

  private final InsertRowStatement insertRowStatement;

  public InsertTableStatement(IClientSession clientSession, Insert insert) {
    this.database = parseDatabase(clientSession, insert);
    this.table = parseTable(insert);
    insertRowStatement = parseInsert(insert);
  }

  private String parseDatabase(IClientSession clientSession, Insert insert) {
    String database = clientSession.getDatabaseName();
    if (database == null) {
      database = insert.getTable().getName().getPrefix().get().getSuffix();
    }
    return database;
  }

  private String parseTable(Insert insert) {
    return insert.getTable().getName().getSuffix().toString();
  }

  private InsertRowStatement parseInsert(Insert insert) {
    InsertRowStatement insertStatement = new InsertRowStatement();
    String tableName = table;
    TsTable table = DataNodeTableCache.getInstance().getTable(database, tableName);
    List<Expression> values =
        ((Row) (((Values) (insert.getQuery().getQueryBody())).getRows().get(0))).getItems();
    Map<String, String> idColumnMap = new HashMap<>();
    Map<String, String> attrColumnMap = new HashMap<>();
    Map<String, Object> measurementColumnMap = new HashMap<>();
    long time = 0L;
    boolean hasColumn = insert.getColumns().isPresent();
    int size = hasColumn ? insert.getColumns().get().size() : table.getColumnNum();
    List<Identifier> columnNameList = hasColumn ? insert.getColumns().get() : null;
    for (int i = 0; i < size; i++) {
      String columnName =
          hasColumn
              ? columnNameList.get(i).getValue()
              : table.getColumnList().get(i).getColumnName();
      TsTableColumnSchema columnSchema = table.getColumnSchema(columnName);
      if (columnSchema == null) {
        throw new SemanticException(String.format("Unknown Column %s", columnName));
      }
      TsTableColumnCategory category = table.getColumnSchema(columnName).getColumnCategory();
      if (category.equals(TsTableColumnCategory.ID)) {
        idColumnMap.put(columnName, ((Identifier) values.get(i)).getValue());
      } else if (category.equals(TsTableColumnCategory.ATTRIBUTE)) {
        attrColumnMap.put(columnName, ((Identifier) values.get(i)).getValue());
      } else if (category.equals(TsTableColumnCategory.MEASUREMENT)) {
        measurementColumnMap.put(columnName, ((LongLiteral) values.get(i)).getValue());
      } else {
        time = Long.parseLong(((LongLiteral) values.get(i)).getValue());
      }
    }
    String[] deviceIds = new String[table.getIdNums() + 3];
    deviceIds[0] = PATH_ROOT;
    deviceIds[1] = database;
    deviceIds[2] = tableName;
    String[] measurements = new String[measurementColumnMap.size()];
    Object[] valueList = new Object[measurements.length];
    MeasurementSchema[] schemas = new MeasurementSchema[measurements.length];
    TSDataType[] dataTypes = new TSDataType[measurements.length];
    int idIndex = 0;
    int measurementIndex = 0;
    for (int i = 0; i < table.getColumnNum(); i++) {
      TsTableColumnCategory category = table.getColumnList().get(i).getColumnCategory();
      if (category.equals(TsTableColumnCategory.ID)) {
        String id = idColumnMap.get(table.getColumnList().get(i).getColumnName());
        deviceIds[3 + idIndex] = id == null ? "" : id;
        idIndex++;
      } else if (category.equals(TsTableColumnCategory.MEASUREMENT)) {
        String measurement = table.getColumnList().get(i).getColumnName();
        if (measurementColumnMap.containsKey(measurement)) {
          measurements[measurementIndex] = measurement;
          valueList[measurementIndex] = measurementColumnMap.get(measurement);
          schemas[measurementIndex] =
              ((MeasurementColumnSchema) table.getColumnList().get(i)).getMeasurementSchema();
          dataTypes[measurementIndex] = schemas[measurementIndex].getType();
          measurementIndex++;
        }
      }
    }

    this.deviceIdList =
        Collections.singletonList(Arrays.copyOfRange(deviceIds, 3, deviceIds.length));
    this.attributeNameList = new ArrayList<>(attrColumnMap.keySet());
    this.attributeValueList =
        Collections.singletonList(
            attributeNameList.stream().map(attrColumnMap::get).collect(Collectors.toList()));

    insertStatement.setDevicePath(new PartialPath(deviceIds));
    TimestampPrecisionUtils.checkTimestampPrecision(time);
    insertStatement.setTime(time);
    insertStatement.setMeasurements(measurements);
    insertStatement.setDataTypes(new TSDataType[insertStatement.getMeasurements().length]);
    insertStatement.setValues(valueList);
    insertStatement.setNeedInferType(true);
    insertStatement.setAligned(true);
    insertStatement.setMeasurementSchemas(schemas);
    insertStatement.setDataTypes(dataTypes);
    try {
      for (int i = 0; i < measurements.length; i++) {
        insertStatement.selfCheckDataTypes(i);
      }
      insertStatement.updateAfterSchemaValidation();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    return insertStatement;
  }

  public InsertRowStatement getInsertRowStatement() {
    return insertRowStatement;
  }

  @Override
  public List<PartialPath> getPaths() {
    return insertRowStatement.getPaths();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTable(this, context);
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public String getTableName() {
    return table;
  }

  @Override
  public List<String[]> getDeviceIdList() {
    return deviceIdList;
  }

  @Override
  public List<String> getAttributeColumnNameList() {
    return attributeNameList;
  }

  @Override
  public List<List<String>> getAttributeValue() {
    return attributeValueList;
  }
}
