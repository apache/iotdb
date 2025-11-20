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
import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class InsertStatementTest {
  @Mock private Metadata metadata;
  @Mock private MPPQueryContext queryContext;
  @Mock private SessionInfo sessionInfo;
  private TableSchema tableSchema;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    List<ColumnSchema> columnSchemas;
    columnSchemas =
        Arrays.asList(
            new ColumnSchema(
                "id1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG),
            new ColumnSchema(
                "id2", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG),
            new ColumnSchema(
                "id3", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG),
            new ColumnSchema(
                "attr1",
                TypeFactory.getType(TSDataType.STRING),
                false,
                TsTableColumnCategory.ATTRIBUTE),
            new ColumnSchema(
                "attr2",
                TypeFactory.getType(TSDataType.STRING),
                false,
                TsTableColumnCategory.ATTRIBUTE),
            new ColumnSchema(
                "m1", TypeFactory.getType(TSDataType.DOUBLE), false, TsTableColumnCategory.FIELD),
            new ColumnSchema(
                "m2", TypeFactory.getType(TSDataType.DOUBLE), false, TsTableColumnCategory.FIELD),
            new ColumnSchema(
                "m3", TypeFactory.getType(TSDataType.INT64), false, TsTableColumnCategory.FIELD));
    tableSchema = new TableSchema("table1", columnSchemas);

    TsTable tsTable = convertTableSchemaToTsTable(tableSchema);
    DataNodeTableCache.getInstance().preUpdateTable("test", tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable("test", "table1", null);

    when(metadata.validateTableHeaderSchema(
            any(String.class),
            any(TableSchema.class),
            any(MPPQueryContext.class),
            any(Boolean.class),
            any(Boolean.class)))
        .thenReturn(Optional.of(tableSchema));

    doAnswer(
            invocation -> {
              String database = invocation.getArgument(0);
              InsertNodeMeasurementInfo measurementInfo = invocation.getArgument(1);
              MPPQueryContext context = invocation.getArgument(2);
              boolean allowCreateTable = invocation.getArgument(3);
              TableHeaderSchemaValidator.MeasurementValidator measurementValidator =
                  invocation.getArgument(4);
              TableHeaderSchemaValidator.TagColumnHandler tagColumnHandler =
                  invocation.getArgument(5);
              TableHeaderSchemaValidator.getInstance()
                  .validateInsertNodeMeasurements(
                      database,
                      measurementInfo,
                      context,
                      allowCreateTable,
                      measurementValidator,
                      tagColumnHandler);
              return null;
            })
        .when(metadata)
        .validateInsertNodeMeasurements(
            anyString(),
            any(InsertNodeMeasurementInfo.class),
            any(MPPQueryContext.class),
            anyBoolean(),
            any(TableHeaderSchemaValidator.MeasurementValidator.class),
            any(TableHeaderSchemaValidator.TagColumnHandler.class));

    when(queryContext.getSession()).thenReturn(sessionInfo);
    when(queryContext.getDatabaseName()).thenReturn(Optional.of("test"));
    when(sessionInfo.getDatabaseName()).thenReturn(Optional.of("test"));
  }

  private TsTable convertTableSchemaToTsTable(TableSchema tableSchema) {
    TsTable tsTable = new TsTable(tableSchema.getTableName());
    for (ColumnSchema columnSchema : tableSchema.getColumns()) {
      TSDataType dataType = InternalTypeManager.getTSDataType(columnSchema.getType());
      TsTableColumnCategory category = columnSchema.getColumnCategory();

      switch (category) {
        case TAG:
          tsTable.addColumnSchema(new TagColumnSchema(columnSchema.getName(), dataType));
          break;
        case ATTRIBUTE:
          tsTable.addColumnSchema(new AttributeColumnSchema(columnSchema.getName(), dataType));
          break;
        case FIELD:
        default:
          tsTable.addColumnSchema(
              new FieldColumnSchema(
                  columnSchema.getName(),
                  dataType,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED));
          break;
      }
    }
    return tsTable;
  }

  @Test
  public void testValidate() {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setDevicePath(new PartialPath(new String[] {"table1"}));
    // wrong id order
    // miss id3
    // the type of m2 is wrong
    insertRowStatement.setMeasurements(new String[] {"id2", "attr2", "m1", "m2", "id1"});
    insertRowStatement.setDataTypes(
        new TSDataType[] {
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.DOUBLE,
          TSDataType.BLOB,
          TSDataType.STRING
        });
    insertRowStatement.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.TAG
        });
    insertRowStatement.setValues(new String[] {"id2", "attr2", "m1", "m2", "id1"});
    InsertRow insertRow = new InsertRow(insertRowStatement, queryContext);

    insertRow.validateTableSchema(metadata, queryContext);

    // id3 should be added into the statement to generate right DeviceIds
    assertArrayEquals(
        new String[] {"id1", "id2", "id3", "attr2", "m1", null},
        insertRowStatement.getMeasurements());
    assertArrayEquals(
        new String[] {"id1", "id2", null, "attr2", "m1", null}, insertRowStatement.getValues());
    assertArrayEquals(
        new TSDataType[] {
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.DOUBLE,
          null
        },
        insertRowStatement.getDataTypes());
    assertArrayEquals(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.FIELD,
          TsTableColumnCategory.FIELD
        },
        insertRowStatement.getColumnCategories());
  }

  @Test
  public void testConflictCategory() {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setDevicePath(new PartialPath(new String[] {"table1"}));

    // category is ID in row but ATTRIBUTE in table
    insertRowStatement.setMeasurements(new String[] {"id1"});
    insertRowStatement.setDataTypes(new TSDataType[] {TSDataType.STRING});
    insertRowStatement.setColumnCategories(new TsTableColumnCategory[] {TsTableColumnCategory.TAG});
    insertRowStatement.setValues(new String[] {"id1"});
    InsertRow insertRow = new InsertRow(insertRowStatement, null);

    List<ColumnSchema> columnSchemas;
    columnSchemas =
        Collections.singletonList(
            new ColumnSchema(
                "id1",
                TypeFactory.getType(TSDataType.STRING),
                false,
                TsTableColumnCategory.ATTRIBUTE));
    tableSchema = new TableSchema("table1", columnSchemas);

    TsTable tsTable = convertTableSchemaToTsTable(tableSchema);
    DataNodeTableCache.getInstance().preUpdateTable("test", tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable("test", "table1", null);

    when(metadata.validateTableHeaderSchema(
            any(String.class),
            any(TableSchema.class),
            any(MPPQueryContext.class),
            any(Boolean.class),
            any(Boolean.class)))
        .thenReturn(Optional.of(tableSchema));

    assertThrows(
        SemanticException.class, () -> insertRow.validateTableSchema(metadata, queryContext));
  }

  @Test
  public void testMissingIdColumn() {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setDevicePath(new PartialPath(new String[] {"table1"}));

    // id1 not in the table
    insertRowStatement.setMeasurements(new String[] {"id1"});
    insertRowStatement.setDataTypes(new TSDataType[] {TSDataType.STRING});
    insertRowStatement.setColumnCategories(new TsTableColumnCategory[] {TsTableColumnCategory.TAG});
    insertRowStatement.setValues(new String[] {"id1"});
    InsertRow insertRow = new InsertRow(insertRowStatement, null);

    List<ColumnSchema> columnSchemas;
    columnSchemas =
        Collections.singletonList(
            new ColumnSchema(
                "id2", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    tableSchema = new TableSchema("table1", columnSchemas);

    TsTable tsTable = convertTableSchemaToTsTable(tableSchema);
    DataNodeTableCache.getInstance().preUpdateTable("test", tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable("test", "table1", null);

    when(metadata.validateTableHeaderSchema(
            any(String.class),
            any(TableSchema.class),
            any(MPPQueryContext.class),
            any(Boolean.class),
            any(Boolean.class)))
        .thenReturn(Optional.of(tableSchema));

    assertThrows(
        SemanticException.class, () -> insertRow.validateTableSchema(metadata, queryContext));
  }
}
