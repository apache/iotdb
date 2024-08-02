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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;

import org.apache.tsfile.enums.TSDataType;
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
                "id1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.ID),
            new ColumnSchema(
                "id2", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.ID),
            new ColumnSchema(
                "id3", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.ID),
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
                "m1",
                TypeFactory.getType(TSDataType.DOUBLE),
                false,
                TsTableColumnCategory.MEASUREMENT),
            new ColumnSchema(
                "m2",
                TypeFactory.getType(TSDataType.DOUBLE),
                false,
                TsTableColumnCategory.MEASUREMENT),
            new ColumnSchema(
                "m3",
                TypeFactory.getType(TSDataType.INT64),
                false,
                TsTableColumnCategory.MEASUREMENT));
    tableSchema = new TableSchema("table1", columnSchemas);
    when(metadata.validateTableHeaderSchema(
            any(String.class),
            any(TableSchema.class),
            any(MPPQueryContext.class),
            any(Boolean.class)))
        .thenReturn(Optional.of(tableSchema));
    when(queryContext.getSession()).thenReturn(sessionInfo);
    when(queryContext.getDatabaseName()).thenReturn(Optional.of("test"));
    when(sessionInfo.getDatabaseName()).thenReturn(Optional.of("test"));
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
          TSDataType.INT64,
          TSDataType.STRING
        });
    insertRowStatement.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.ID,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.MEASUREMENT,
          TsTableColumnCategory.MEASUREMENT,
          TsTableColumnCategory.ID
        });
    insertRowStatement.setValues(new String[] {"id2", "attr2", "m1", "m2", "id1"});
    InsertRow insertRow = new InsertRow(insertRowStatement, queryContext);

    insertRow.validateTableSchema(metadata, queryContext);

    // id3 should be added into the statement to generate right DeviceIds
    assertArrayEquals(
        new String[] {"id1", "id2", "id3", "m1", null, "attr2"},
        insertRowStatement.getMeasurements());
    assertArrayEquals(
        new String[] {"id1", "id2", null, "m1", null, "attr2"}, insertRowStatement.getValues());
    assertArrayEquals(
        new TSDataType[] {
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.DOUBLE,
          null,
          TSDataType.STRING
        },
        insertRowStatement.getDataTypes());
    assertArrayEquals(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.ID,
          TsTableColumnCategory.ID,
          TsTableColumnCategory.ID,
          TsTableColumnCategory.MEASUREMENT,
          TsTableColumnCategory.MEASUREMENT,
          TsTableColumnCategory.ATTRIBUTE
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
    insertRowStatement.setColumnCategories(new TsTableColumnCategory[] {TsTableColumnCategory.ID});
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
    when(metadata.validateTableHeaderSchema(
            any(String.class),
            any(TableSchema.class),
            any(MPPQueryContext.class),
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
    insertRowStatement.setColumnCategories(new TsTableColumnCategory[] {TsTableColumnCategory.ID});
    insertRowStatement.setValues(new String[] {"id1"});
    InsertRow insertRow = new InsertRow(insertRowStatement, null);

    List<ColumnSchema> columnSchemas;
    columnSchemas =
        Collections.singletonList(
            new ColumnSchema(
                "id2", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.ID));
    tableSchema = new TableSchema("table1", columnSchemas);
    when(metadata.validateTableHeaderSchema(
            any(String.class),
            any(TableSchema.class),
            any(MPPQueryContext.class),
            any(Boolean.class)))
        .thenReturn(Optional.of(tableSchema));

    assertThrows(
        SemanticException.class, () -> insertRow.validateTableSchema(metadata, queryContext));
  }
}
