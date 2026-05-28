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

package com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;

import static org.apache.iotdb.rpc.TSStatusCode.COLUMN_NOT_EXISTS;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_ALREADY_EXISTS;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_INCOMPATIBLE;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_NOT_EXISTS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateWritableViewProcedureTest {

  private static final String DATABASE = "database1";
  private static final String VIEW_NAME = "view1";
  private static final String SOURCE_TABLE_NAME = "table1";
  private static final String SOURCE_COLUMN_NAME = "s1";
  private static final String VIEW_COLUMN_NAME = "v1";

  @Test
  public void serializeDeserializeTest() throws IOException {
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, true);
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE, view, true, false, Collections.singletonMap(SOURCE_COLUMN_NAME, "comment"));

    final CreateWritableViewProcedure deserializedProcedure = serializeDeserialize(procedure);

    assertCreateWritableViewProcedureEquals(procedure, deserializedProcedure);
  }

  @Test
  public void serializeDeserializeAfterCascadeInitializationTest() throws Exception {
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    sourceTable.addProp(TsTable.TTL_PROPERTY, "100");
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, true);
    view.addProp(TsTable.TTL_PROPERTY, "200");
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);

    procedure.checkTableExistence(mockEnv(sourceTable));

    Assert.assertFalse(procedure.isFailed());
    Assert.assertNotNull(procedure.getOriginalTable());

    final CreateWritableViewProcedure deserializedProcedure = serializeDeserialize(procedure);

    assertCreateWritableViewProcedureEquals(procedure, deserializedProcedure);
  }

  @Test
  public void serializeDeserializeShouldAllowOriginalTableWithoutOldSnapshot() throws IOException {
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, true);
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    setFieldValue(procedure, CreateWritableViewProcedure.class, "originalTable", sourceTable);

    final CreateWritableViewProcedure deserializedProcedure = serializeDeserialize(procedure);

    Assert.assertNull(
        getFieldValue(
            deserializedProcedure, CreateWritableViewProcedure.class, "oldOriginalTable"));
    assertTsTableEquals(sourceTable, deserializedProcedure.getOriginalTable());
  }

  @Test
  public void checkTableExistenceShouldInheritSourceComments() throws Exception {
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false);
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);

    procedure.checkTableExistence(mockEnv(sourceTable));

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(
        "source-table-comment", view.getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "source-column-comment",
        view.getColumnSchema(SOURCE_COLUMN_NAME).getProps().get(TsTable.COMMENT_KEY));
    Assert.assertNull(procedure.getOriginalTable());
  }

  @Test
  public void checkTableExistenceShouldApplyExplicitCommentsAndCascadeToSource() throws Exception {
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, true);
    view.addProp(TsTable.COMMENT_KEY, "view-table-comment");
    view.setViewColumnToSourceColumnMap(
        new LinkedHashMap<>(Collections.singletonMap(VIEW_COLUMN_NAME, SOURCE_COLUMN_NAME)));

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE,
            view,
            true,
            false,
            Collections.singletonMap(VIEW_COLUMN_NAME, "view-column-comment"));

    procedure.checkTableExistence(mockEnv(sourceTable));

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals("view-table-comment", view.getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "view-column-comment",
        view.getColumnSchema(VIEW_COLUMN_NAME).getProps().get(TsTable.COMMENT_KEY));
    Assert.assertEquals(
        SOURCE_COLUMN_NAME,
        view.getColumnSchema(VIEW_COLUMN_NAME).getProps().get(ViewColumnSchemaUtils.SOURCE_NAME));
    Assert.assertEquals(
        "view-table-comment",
        procedure.getOriginalTable().getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "view-column-comment",
        procedure
            .getOriginalTable()
            .getColumnSchema(SOURCE_COLUMN_NAME)
            .getProps()
            .get(TsTable.COMMENT_KEY));
    Assert.assertEquals(
        "source-table-comment", sourceTable.getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "source-column-comment",
        sourceTable.getColumnSchema(SOURCE_COLUMN_NAME).getProps().get(TsTable.COMMENT_KEY));
    final TsTable rollbackOriginalTable =
        (TsTable)
            getFieldValue(procedure, CreateWritableViewProcedure.class, "rollbackOriginalTable");
    Assert.assertEquals(
        "source-table-comment",
        rollbackOriginalTable.getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "source-column-comment",
        rollbackOriginalTable
            .getColumnSchema(SOURCE_COLUMN_NAME)
            .getProps()
            .get(TsTable.COMMENT_KEY));
  }

  @Test
  public void checkTableExistenceShouldKeepColumnSourceNameWithoutMapping() throws Exception {
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false);
    view.setViewColumnToSourceColumnMap(
        new LinkedHashMap<>(Collections.singletonMap(VIEW_COLUMN_NAME, SOURCE_COLUMN_NAME)));

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);

    procedure.checkTableExistence(mockEnv(sourceTable));

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(
        SOURCE_COLUMN_NAME,
        view.getColumnSchema(VIEW_COLUMN_NAME).getProps().get(ViewColumnSchemaUtils.SOURCE_NAME));
    view.setViewColumnToSourceColumnMap(null);
    Assert.assertEquals(SOURCE_COLUMN_NAME, view.getOriginalColumnName(VIEW_COLUMN_NAME));
  }

  @Test
  public void checkTableExistenceShouldRoundTripCascadeOriginalTable() throws Exception {
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, true);
    view.addProp(TsTable.COMMENT_KEY, "view-table-comment");
    view.setViewColumnToSourceColumnMap(
        new LinkedHashMap<>(Collections.singletonMap(VIEW_COLUMN_NAME, SOURCE_COLUMN_NAME)));

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE,
            view,
            true,
            false,
            Collections.singletonMap(VIEW_COLUMN_NAME, "view-column-comment"));

    procedure.checkTableExistence(mockEnv(sourceTable));

    Assert.assertFalse(procedure.isFailed());
    final CreateWritableViewProcedure deserializedProcedure = serializeDeserialize(procedure);
    assertCreateWritableViewProcedureEquals(procedure, deserializedProcedure);
    Assert.assertEquals(
        "view-table-comment",
        deserializedProcedure.getOriginalTable().getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "view-column-comment",
        deserializedProcedure
            .getOriginalTable()
            .getColumnSchema(SOURCE_COLUMN_NAME)
            .getProps()
            .get(TsTable.COMMENT_KEY));
  }

  @Test
  public void checkTableExistenceShouldKeepMaterializedColumnComments() throws Exception {
    final TsTable sourceTable = createSourceTable("source-table-comment", "source-column-comment");
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, true);
    view.setViewColumnToSourceColumnMap(
        new LinkedHashMap<>(Collections.singletonMap(VIEW_COLUMN_NAME, SOURCE_COLUMN_NAME)));
    view.addColumnSchema(new TimeColumnSchema(TsTable.TIME_COLUMN_NAME, TSDataType.TIMESTAMP));
    final FieldColumnSchema fieldColumnSchema =
        new FieldColumnSchema(
            VIEW_COLUMN_NAME, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
    fieldColumnSchema.getProps().put(TsTable.COMMENT_KEY, "materialized-column-comment");
    view.addColumnSchema(fieldColumnSchema);

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);

    procedure.checkTableExistence(mockEnv(sourceTable));

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(
        "source-table-comment", view.getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "source-table-comment",
        procedure.getOriginalTable().getPropValue(TsTable.COMMENT_KEY).orElse(null));
    Assert.assertEquals(
        "materialized-column-comment",
        view.getColumnSchema(VIEW_COLUMN_NAME).getProps().get(TsTable.COMMENT_KEY));
    Assert.assertEquals(
        "materialized-column-comment",
        procedure
            .getOriginalTable()
            .getColumnSchema(SOURCE_COLUMN_NAME)
            .getProps()
            .get(TsTable.COMMENT_KEY));
    Assert.assertEquals(
        "source-column-comment",
        sourceTable.getColumnSchema(SOURCE_COLUMN_NAME).getProps().get(TsTable.COMMENT_KEY));
  }

  @Test
  public void checkTableExistenceShouldRejectReplaceWhenBaseTableAlreadyExists() throws Exception {
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE, new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false), true, false);

    procedure.checkTableExistence(
        mockEnv(
            Optional.of(new TsTable(VIEW_NAME)),
            Optional.of(createSourceTable("source-table-comment", "source-column-comment"))));

    assertProcedureFailure(procedure, TABLE_ALREADY_EXISTS.getStatusCode(), "already exists");
  }

  @Test
  public void checkTableExistenceShouldRejectMissingSourceTable() throws Exception {
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE, new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false), true, false);

    procedure.checkTableExistence(mockEnv(Optional.empty(), Optional.empty()));

    assertProcedureFailure(procedure, TABLE_NOT_EXISTS.getStatusCode(), "does not exist");
  }

  @Test
  public void checkTableExistenceShouldRejectWritableViewSourceTable() throws Exception {
    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE, new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false), true, false);

    procedure.checkTableExistence(
        mockEnv(
            Optional.empty(),
            Optional.of(new WritableView(SOURCE_TABLE_NAME, DATABASE, "base_table", false))));

    assertProcedureFailure(procedure, TABLE_INCOMPATIBLE.getStatusCode(), "not a base table");
  }

  @Test
  public void checkTableExistenceShouldRejectTreeViewSourceTable() throws Exception {
    final TsTable sourceTable = new TsTable(SOURCE_TABLE_NAME);
    sourceTable.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.database1.**");

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(
            DATABASE, new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false), true, false);

    procedure.checkTableExistence(mockEnv(Optional.empty(), Optional.of(sourceTable)));

    assertProcedureFailure(procedure, TABLE_INCOMPATIBLE.getStatusCode(), "not a base table");
  }

  @Test
  public void checkTableExistenceShouldRejectMissingMappedSourceColumn() throws Exception {
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false);
    view.setViewColumnToSourceColumnMap(
        new LinkedHashMap<>(Collections.singletonMap(VIEW_COLUMN_NAME, "missing_column")));

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);

    procedure.checkTableExistence(
        mockEnv(
            Optional.empty(),
            Optional.of(createSourceTable("source-table-comment", "source-column-comment"))));

    assertProcedureFailure(procedure, COLUMN_NOT_EXISTS.getStatusCode(), "missing_column");
  }

  @Test
  public void checkTableExistenceShouldAutoAddTimeColumnWhenProjectionOmitsTime() throws Exception {
    final WritableView view = new WritableView(VIEW_NAME, DATABASE, SOURCE_TABLE_NAME, false);
    view.setViewColumnToSourceColumnMap(
        new LinkedHashMap<>(Collections.singletonMap(VIEW_COLUMN_NAME, SOURCE_COLUMN_NAME)));

    final CreateWritableViewProcedure procedure =
        new CreateWritableViewProcedure(DATABASE, view, true, false);

    procedure.checkTableExistence(
        mockEnv(
            Optional.empty(),
            Optional.of(createSourceTable("source-table-comment", "source-column-comment"))));

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(2, view.getColumnNum());
    Assert.assertTrue(view.getColumnSchema(TsTable.TIME_COLUMN_NAME) instanceof TimeColumnSchema);
    Assert.assertEquals(
        TsTable.TIME_COLUMN_NAME,
        view.getViewColumnToSourceColumnMap().get(TsTable.TIME_COLUMN_NAME));
  }

  private ConfigNodeProcedureEnv mockEnv(final TsTable sourceTable) throws Exception {
    return mockEnv(Optional.empty(), Optional.of(sourceTable));
  }

  private ConfigNodeProcedureEnv mockEnv(
      final Optional<TsTable> existingTable, final Optional<TsTable> sourceTable) throws Exception {
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists(DATABASE, VIEW_NAME))
        .thenReturn(
            existingTable.map(
                table -> new org.apache.tsfile.utils.Pair<>(table, TableNodeStatus.USING)));
    when(clusterSchemaManager.getTableIfExists(DATABASE, SOURCE_TABLE_NAME))
        .thenReturn(sourceTable);
    return env;
  }

  private TsTable createSourceTable(final String tableComment, final String columnComment) {
    final TsTable sourceTable = new TsTable(SOURCE_TABLE_NAME);
    sourceTable.addColumnSchema(
        new TimeColumnSchema(TsTable.TIME_COLUMN_NAME, TSDataType.TIMESTAMP));
    final FieldColumnSchema fieldColumnSchema =
        new FieldColumnSchema(
            SOURCE_COLUMN_NAME, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
    fieldColumnSchema.getProps().put(TsTable.COMMENT_KEY, columnComment);
    sourceTable.addColumnSchema(fieldColumnSchema);
    sourceTable.addProp(TsTable.COMMENT_KEY, tableComment);
    return sourceTable;
  }

  private void assertCreateWritableViewProcedureEquals(
      final CreateWritableViewProcedure expected, final CreateWritableViewProcedure actual) {
    Assert.assertEquals(expected.getDatabase(), actual.getDatabase());
    Assert.assertTrue(actual.getTable() instanceof WritableView);
    assertWritableViewEquals((WritableView) expected.getTable(), (WritableView) actual.getTable());
    Assert.assertEquals(
        getFieldValue(expected, CreateTableViewProcedure.class, "replace"),
        getFieldValue(actual, CreateTableViewProcedure.class, "replace"));
    Assert.assertEquals(
        getFieldValue(expected, CreateWritableViewProcedure.class, "viewColumnCommentMap"),
        getFieldValue(actual, CreateWritableViewProcedure.class, "viewColumnCommentMap"));
    assertNullableTsTableEquals(
        (TsTable) getFieldValue(expected, CreateWritableViewProcedure.class, "oldOriginalTable"),
        (TsTable) getFieldValue(actual, CreateWritableViewProcedure.class, "oldOriginalTable"));
    assertNullableTsTableEquals(
        (TsTable)
            getFieldValue(expected, CreateWritableViewProcedure.class, "rollbackOriginalTable"),
        (TsTable)
            getFieldValue(actual, CreateWritableViewProcedure.class, "rollbackOriginalTable"));
    assertNullableTsTableEquals(
        (TsTable) getFieldValue(expected, CreateTableViewProcedure.class, "oldView"),
        (TsTable) getFieldValue(actual, CreateTableViewProcedure.class, "oldView"));
    Assert.assertEquals(
        getFieldValue(expected, CreateTableViewProcedure.class, "oldStatus"),
        getFieldValue(actual, CreateTableViewProcedure.class, "oldStatus"));
    assertNullableTsTableEquals(expected.getOriginalTable(), actual.getOriginalTable());
  }

  private CreateWritableViewProcedure serializeDeserialize(
      final CreateWritableViewProcedure procedure) throws IOException {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    procedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.CREATE_WRITABLE_VIEW_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final CreateWritableViewProcedure deserializedProcedure =
        new CreateWritableViewProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);
    return deserializedProcedure;
  }

  private void assertWritableViewEquals(final WritableView expected, final WritableView actual) {
    assertTsTableEquals(expected, actual);
    Assert.assertEquals(expected.getSourceTableDatabase(), actual.getSourceTableDatabase());
    Assert.assertEquals(expected.getSourceTableName(), actual.getSourceTableName());
    Assert.assertEquals(expected.isSchemaCascade(), actual.isSchemaCascade());
    Assert.assertEquals(
        expected.getViewColumnToSourceColumnMap(), actual.getViewColumnToSourceColumnMap());
  }

  private void assertNullableTsTableEquals(final TsTable expected, final TsTable actual) {
    if (expected == null || actual == null) {
      Assert.assertEquals(expected, actual);
      return;
    }
    assertTsTableEquals(expected, actual);
  }

  private void assertTsTableEquals(final TsTable expected, final TsTable actual) {
    Assert.assertEquals(expected.getClass(), actual.getClass());
    Assert.assertEquals(expected.getTableName(), actual.getTableName());
    Assert.assertEquals(expected.getColumnNum(), actual.getColumnNum());
    Assert.assertEquals(expected.getTagNum(), actual.getTagNum());
    Assert.assertEquals(expected.getFieldNum(), actual.getFieldNum());
    Assert.assertEquals(expected.getProps(), actual.getProps());
    Assert.assertEquals(expected.getColumnList().size(), actual.getColumnList().size());
    for (int i = 0; i < expected.getColumnList().size(); i++) {
      Assert.assertEquals(
          expected.getColumnList().get(i).getClass(), actual.getColumnList().get(i).getClass());
      Assert.assertEquals(
          expected.getColumnList().get(i).getColumnName(),
          actual.getColumnList().get(i).getColumnName());
      Assert.assertEquals(
          expected.getColumnList().get(i).getDataType(),
          actual.getColumnList().get(i).getDataType());
      Assert.assertEquals(
          expected.getColumnList().get(i).getColumnCategory(),
          actual.getColumnList().get(i).getColumnCategory());
      Assert.assertEquals(
          expected.getColumnList().get(i).getProps(), actual.getColumnList().get(i).getProps());
    }
  }

  private Object getFieldValue(
      final Object target, final Class<?> ownerClass, final String fieldName) {
    try {
      final Field field = ownerClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(target);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private void setFieldValue(
      final Object target, final Class<?> ownerClass, final String fieldName, final Object value) {
    try {
      final Field field = ownerClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private void assertProcedureFailure(
      final CreateWritableViewProcedure procedure,
      final int expectedErrorCode,
      final String expectedMessagePart) {
    Assert.assertTrue(procedure.isFailed());
    Assert.assertTrue(procedure.getFailure().getCause() instanceof IoTDBException);
    final IoTDBException cause = (IoTDBException) procedure.getFailure().getCause();
    Assert.assertEquals(expectedErrorCode, cause.getErrorCode());
    Assert.assertTrue(cause.getMessage(), cause.getMessage().contains(expectedMessagePart));
  }
}
