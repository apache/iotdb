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
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AbstractAlterOrDropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.service.ConfigNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.rpc.TSStatusCode.COLUMN_NOT_EXISTS;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_ALREADY_EXISTS;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WritableViewProcedureTest {

  @Test
  public void addWritableViewColumnSerializeDeserializeTest() throws IOException {
    assertAddWritableViewColumnSerializeDeserialize(
        new AddWritableViewColumnProcedure(
            "database1",
            "table1",
            "0",
            Collections.singletonList(new TagColumnSchema("id", TSDataType.STRING)),
            false),
        ProcedureType.ADD_WRITABLE_VIEW_COLUMN_PROCEDURE,
        new AddWritableViewColumnProcedure(false));
    assertAddWritableViewColumnSerializeDeserialize(
        new AddWritableViewColumnProcedure(
            "database1",
            "table1",
            "0",
            Collections.singletonList(new TagColumnSchema("id", TSDataType.STRING)),
            true),
        ProcedureType.PIPE_ENRICHED_ADD_WRITABLE_VIEW_COLUMN_PROCEDURE,
        new AddWritableViewColumnProcedure(true));
  }

  @Test
  public void addWritableViewAttributeColumnWithViewSyntaxTest() throws Exception {
    final AddWritableViewColumnProcedure procedure =
        new AddWritableViewColumnProcedure(
            "database1",
            "view1",
            "0",
            Collections.singletonList(new AttributeColumnSchema("label", TSDataType.STRING)),
            false);

    final WritableView writableView = new WritableView("view1", "database1", "source1", false);
    final TsTable sourceTable = new TsTable("source1");
    sourceTable.addColumnSchema(new AttributeColumnSchema("label", TSDataType.STRING));

    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableIfExists("database1", "view1"))
        .thenReturn(Optional.of(writableView));
    when(clusterSchemaManager.getTableIfExists("database1", "source1"))
        .thenReturn(Optional.of(sourceTable));
    when(clusterSchemaManager.tableColumnCheckForColumnExtension(
            eq("database1"), eq("view1"), anyList(), eq(TableType.WRITABLE_VIEW), anyBoolean()))
        .thenReturn(new Pair<>(StatusUtils.OK, writableView));

    procedure.columnCheck(env);

    Assert.assertFalse(procedure.isFailed());
    final List<TsTableColumnSchema> addedColumnList =
        getFieldValue(procedure, AddTableColumnProcedure.class, "addedColumnList");
    Assert.assertEquals(1, addedColumnList.size());
    Assert.assertEquals(ATTRIBUTE, addedColumnList.get(0).getColumnCategory());
  }

  @Test
  public void addWritableViewColumnWithSchemaCascadeShouldExtendSourceAsBaseTable()
      throws Exception {
    final AddWritableViewColumnProcedure procedure =
        new AddWritableViewColumnProcedure(
            "database1",
            "view1",
            "0",
            Collections.singletonList(
                new org.apache.iotdb.commons.schema.table.column.FieldColumnSchema(
                    "pressure", TSDataType.INT32, null, null)),
            false);

    final WritableView writableView = new WritableView("view1", "database1", "source1", true);
    final TsTable sourceTable = new TsTable("source1");
    final WritableView expandedView = new WritableView(writableView);
    final TsTable expandedSourceTable = new TsTable(sourceTable);

    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableIfExists("database1", "view1"))
        .thenReturn(Optional.of(writableView));
    when(clusterSchemaManager.getTableIfExists("database1", "source1"))
        .thenReturn(Optional.of(sourceTable));
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.of(new Pair<>(sourceTable, TableNodeStatus.USING)));
    when(clusterSchemaManager.tableColumnCheckForColumnExtension(
            eq("database1"), eq("view1"), anyList(), eq(TableType.WRITABLE_VIEW), anyBoolean()))
        .thenReturn(new Pair<>(StatusUtils.OK, expandedView));
    when(clusterSchemaManager.tableColumnCheckForColumnExtension(
            eq("database1"), eq("source1"), anyList(), eq(TableType.BASE_TABLE), anyBoolean()))
        .thenReturn(new Pair<>(StatusUtils.OK, expandedSourceTable));

    ConfigNode.setInstance(mockedConfigNode);
    try {
      procedure.columnCheck(env);

      Assert.assertFalse(procedure.isFailed());
      verify(clusterSchemaManager)
          .tableColumnCheckForColumnExtension(
              eq("database1"), eq("source1"), anyList(), eq(TableType.BASE_TABLE), eq(true));
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  @Test
  public void addWritableViewColumnActionMessageShouldIncludeTargetDetails() {
    final AddWritableViewColumnProcedure procedure =
        new AddWritableViewColumnProcedure(
            "database1",
            "view1",
            "0",
            Collections.singletonList(new TagColumnSchema("id", TSDataType.STRING)),
            false);

    final String actionMessage = procedure.getActionMessage();

    Assert.assertTrue(actionMessage.contains("database1"));
    Assert.assertTrue(actionMessage.contains("view1"));
    Assert.assertTrue(actionMessage.contains("id"));
  }

  @Test
  public void setWritableViewPropertiesSerializeDeserializeTest() throws IOException {
    final Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "value1");
    properties.put(TsTable.TTL_PROPERTY, null);
    properties.put(WritableView.SCHEMA_CASCADE, "true");

    assertSetWritableViewPropertiesSerializeDeserialize(
        new SetWritableViewPropertiesProcedure("database1", "table1", "0", properties, false),
        ProcedureType.SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE,
        new SetWritableViewPropertiesProcedure(false));
    assertSetWritableViewPropertiesSerializeDeserialize(
        new SetWritableViewPropertiesProcedure("database1", "table1", "0", properties, true),
        ProcedureType.PIPE_ENRICHED_SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE,
        new SetWritableViewPropertiesProcedure(true));
  }

  @Test
  public void setWritableViewPropertiesShouldUsePreviousSchemaCascadeForSourceResolution()
      throws Exception {
    final Map<String, String> enableCascadeProperties = new HashMap<>();
    enableCascadeProperties.put(TsTable.TTL_PROPERTY, "300");
    enableCascadeProperties.put(WritableView.SCHEMA_CASCADE, "true");
    final SetWritableViewPropertiesProcedure oldCascadeDisabledProcedure =
        new SetWritableViewPropertiesProcedure(
            "database1", "view1", "0", enableCascadeProperties, false);
    oldCascadeDisabledProcedure.setTable(new WritableView("view1", "database1", "source1", false));

    Assert.assertNull(oldCascadeDisabledProcedure.getOriginalDatabase());
    Assert.assertNull(oldCascadeDisabledProcedure.getOriginalTable());

    final Map<String, String> disableCascadeProperties = new HashMap<>();
    disableCascadeProperties.put(TsTable.TTL_PROPERTY, "400");
    disableCascadeProperties.put(WritableView.SCHEMA_CASCADE, "false");
    final SetWritableViewPropertiesProcedure oldCascadeEnabledProcedure =
        new SetWritableViewPropertiesProcedure(
            "database1", "view1", "0", disableCascadeProperties, false);
    oldCascadeEnabledProcedure.setTable(new WritableView("view1", "database1", "source1", true));

    final SetWritableViewPropertiesProcedure overrideDisabledProcedure =
        new SetWritableViewPropertiesProcedure(
            "database1", "view1", "0", disableCascadeProperties, false, false);
    overrideDisabledProcedure.setTable(new WritableView("view1", "database1", "source1", true));
    Assert.assertNull(overrideDisabledProcedure.getOriginalDatabase());
    Assert.assertNull(overrideDisabledProcedure.getOriginalTable());

    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    final TsTable sourceTable = new TsTable("source1");
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.of(new Pair<>(sourceTable, TableNodeStatus.USING)));

    ConfigNode.setInstance(mockedConfigNode);
    try {
      Assert.assertEquals("database1", oldCascadeEnabledProcedure.getOriginalDatabase());
      Assert.assertEquals(sourceTable, oldCascadeEnabledProcedure.getOriginalTable());
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  @Test
  public void setWritableViewPropertiesShouldNotCascadeUsingNewSchemaCascade() throws Exception {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, "100");
    properties.put(WritableView.SCHEMA_CASCADE, "true");
    final SetWritableViewPropertiesProcedure procedure =
        new SetWritableViewPropertiesProcedure("database1", "view1", "0", properties, false);
    procedure.setTable(new WritableView("view1", "database1", "source1", false));

    final WritableView updatedView = new WritableView("view1", "database1", "source1", true);
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.updateTableProperties(
            eq("database1"), eq("view1"), anyMap(), eq(properties), eq(TableType.WRITABLE_VIEW)))
        .thenReturn(new Pair<>(StatusUtils.OK, updatedView));

    procedure.validateTable(env);

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(
        Boolean.FALSE,
        getFieldValue(
            procedure, SetWritableViewPropertiesProcedure.class, "shouldRunSourceProcedure"));
    verify(clusterSchemaManager, never())
        .updateTableProperties(
            eq("database1"), eq("source1"), anyMap(), anyMap(), eq(TableType.BASE_TABLE));
  }

  @Test
  public void setWritableViewPropertiesShouldCascadeOtherPropertiesUsingOldSchemaCascade()
      throws Exception {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, "300");
    properties.put(WritableView.SCHEMA_CASCADE, "false");
    final SetWritableViewPropertiesProcedure procedure =
        new SetWritableViewPropertiesProcedure("database1", "view1", "0", properties, false);
    procedure.setTable(new WritableView("view1", "database1", "source1", true));

    final WritableView updatedView = new WritableView("view1", "database1", "source1", false);
    final TsTable sourceTable = new TsTable("source1");
    final TsTable updatedSourceTable = new TsTable("source1");
    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.of(new Pair<>(sourceTable, TableNodeStatus.USING)));
    when(clusterSchemaManager.updateTableProperties(
            eq("database1"), eq("view1"), anyMap(), eq(properties), eq(TableType.WRITABLE_VIEW)))
        .thenReturn(new Pair<>(StatusUtils.OK, updatedView));
    when(clusterSchemaManager.updateTableProperties(
            eq("database1"), eq("source1"), anyMap(), anyMap(), eq(TableType.BASE_TABLE)))
        .thenReturn(new Pair<>(StatusUtils.OK, updatedSourceTable));

    ConfigNode.setInstance(mockedConfigNode);
    try {
      procedure.validateTable(env);

      Assert.assertFalse(procedure.isFailed());
      Assert.assertEquals(
          Boolean.TRUE,
          getFieldValue(
              procedure, SetWritableViewPropertiesProcedure.class, "shouldRunSourceProcedure"));
      final ArgumentCaptor<Map> sourcePropertiesCaptor = ArgumentCaptor.forClass(Map.class);
      verify(clusterSchemaManager)
          .updateTableProperties(
              eq("database1"),
              eq("source1"),
              anyMap(),
              sourcePropertiesCaptor.capture(),
              eq(TableType.BASE_TABLE));
      Assert.assertEquals("300", sourcePropertiesCaptor.getValue().get(TsTable.TTL_PROPERTY));
      Assert.assertFalse(
          sourcePropertiesCaptor.getValue().containsKey(WritableView.SCHEMA_CASCADE));
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  @Test
  public void setWritableViewPropertiesDeserializeShouldKeepSourceResolutionOverride()
      throws Exception {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, "300");
    properties.put(WritableView.SCHEMA_CASCADE, "false");
    final SetWritableViewPropertiesProcedure procedure =
        new SetWritableViewPropertiesProcedure("database1", "view1", "0", properties, false);
    procedure.setTable(new WritableView("view1", "database1", "source1", false));
    setFieldValue(
        procedure, SetWritableViewPropertiesProcedure.class, "shouldRunSourceProcedure", true);

    final ByteBuffer byteBuffer =
        serializeAndAssertTypeCode(procedure, ProcedureType.SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE);
    final SetWritableViewPropertiesProcedure deserializedProcedure =
        new SetWritableViewPropertiesProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    final TsTable sourceTable = new TsTable("source1");
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.of(new Pair<>(sourceTable, TableNodeStatus.USING)));

    ConfigNode.setInstance(mockedConfigNode);
    try {
      Assert.assertEquals("database1", deserializedProcedure.getOriginalDatabase());
      Assert.assertEquals(sourceTable, deserializedProcedure.getOriginalTable());
      Assert.assertEquals(
          Boolean.TRUE,
          getFieldValue(
              deserializedProcedure,
              SetWritableViewPropertiesProcedure.class,
              "shouldRunSourceProcedure"));
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  @Test
  public void renameWritableViewColumnSerializeDeserializeTest() throws IOException {
    assertSerializeDeserialize(
        new RenameWritableViewColumnProcedure(
            "database1", "table1", "0", "oldName", "newName", false),
        ProcedureType.RENAME_WRITABLE_VIEW_COLUMN_PROCEDURE,
        new RenameWritableViewColumnProcedure(false));
    assertSerializeDeserialize(
        new RenameWritableViewColumnProcedure(
            "database1", "table1", "0", "oldName", "newName", true),
        ProcedureType.PIPE_ENRICHED_RENAME_WRITABLE_VIEW_COLUMN_PROCEDURE,
        new RenameWritableViewColumnProcedure(true));
  }

  @Test
  public void renameWritableViewColumnShouldNotResolveSourceTable() {
    final RenameWritableViewColumnProcedure procedure =
        new RenameWritableViewColumnProcedure(
            "database1", "view1", "0", "oldName", "newName", false);
    procedure.setTable(new WritableView("view1", "database1", "source1", true));

    Assert.assertNull(procedure.getOriginalDatabase());
    Assert.assertNull(procedure.getOriginalTable());
  }

  @Test
  public void renameWritableViewSerializeDeserializeTest() throws IOException {
    assertSerializeDeserialize(
        new RenameWritableViewProcedure("database1", "table1", "0", "newName", false),
        ProcedureType.RENAME_WRITABLE_VIEW_PROCEDURE,
        new RenameWritableViewProcedure(false));
    assertSerializeDeserialize(
        new RenameWritableViewProcedure("database1", "table1", "0", "newName", true),
        ProcedureType.PIPE_ENRICHED_RENAME_WRITABLE_VIEW_PROCEDURE,
        new RenameWritableViewProcedure(true));
  }

  @Test
  public void writableViewProcedureTypeCodesShouldBeNegative() {
    final ProcedureType[] procedureTypes = {
      ProcedureType.CREATE_WRITABLE_VIEW_PROCEDURE,
      ProcedureType.ADD_WRITABLE_VIEW_COLUMN_PROCEDURE,
      ProcedureType.DROP_WRITABLE_VIEW_COLUMN_PROCEDURE,
      ProcedureType.DROP_WRITABLE_VIEW_PROCEDURE,
      ProcedureType.SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE,
      ProcedureType.ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE,
      ProcedureType.RENAME_WRITABLE_VIEW_COLUMN_PROCEDURE,
      ProcedureType.RENAME_WRITABLE_VIEW_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_CREATE_WRITABLE_VIEW_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_ADD_WRITABLE_VIEW_COLUMN_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_DROP_WRITABLE_VIEW_COLUMN_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_DROP_WRITABLE_VIEW_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_SET_WRITABLE_VIEW_PROPERTIES_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_RENAME_WRITABLE_VIEW_COLUMN_PROCEDURE,
      ProcedureType.PIPE_ENRICHED_RENAME_WRITABLE_VIEW_PROCEDURE
    };

    for (final ProcedureType procedureType : procedureTypes) {
      Assert.assertTrue(procedureType.name(), procedureType.getTypeCode() < 0);
    }
  }

  @Test
  public void dropWritableViewColumnSerializeDeserializeTest() throws IOException {
    final DropWritableViewColumnProcedure dropWritableViewColumnProcedure =
        new DropWritableViewColumnProcedure("database1", "table1", "0", "columnName", false);
    setFieldValue(
        dropWritableViewColumnProcedure,
        DropTableColumnProcedure.class,
        "skipOriginalTagCascade",
        true);
    assertSerializeDeserialize(
        dropWritableViewColumnProcedure,
        ProcedureType.DROP_WRITABLE_VIEW_COLUMN_PROCEDURE,
        new DropWritableViewColumnProcedure(false));
    assertSerializeDeserialize(
        new DropWritableViewColumnProcedure("database1", "table1", "0", "columnName", true),
        ProcedureType.PIPE_ENRICHED_DROP_WRITABLE_VIEW_COLUMN_PROCEDURE,
        new DropWritableViewColumnProcedure(true));
  }

  @Test
  public void alterWritableViewColumnDataTypeSerializeDeserializeTest() throws IOException {
    assertSerializeDeserialize(
        new AlterWritableViewColumnDataTypeProcedure(
            "database1", "table1", "0", "columnName", TSDataType.INT64, false),
        ProcedureType.ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE,
        new AlterWritableViewColumnDataTypeProcedure(false));
    assertSerializeDeserialize(
        new AlterWritableViewColumnDataTypeProcedure(
            "database1", "table1", "0", "columnName", TSDataType.INT64, true),
        ProcedureType.PIPE_ENRICHED_ALTER_WRITABLE_VIEW_COLUMN_DATATYPE_PROCEDURE,
        new AlterWritableViewColumnDataTypeProcedure(true));
  }

  @Test
  public void dropWritableViewSerializeDeserializeTest() throws IOException {
    assertSerializeDeserialize(
        new DropWritableViewProcedure("database1", "table1", "0", false),
        ProcedureType.DROP_WRITABLE_VIEW_PROCEDURE,
        new DropWritableViewProcedure(false));
    assertSerializeDeserialize(
        new DropWritableViewProcedure("database1", "table1", "0", true),
        ProcedureType.PIPE_ENRICHED_DROP_WRITABLE_VIEW_PROCEDURE,
        new DropWritableViewProcedure(true));
  }

  @Test
  public void executeForSourceShouldIgnoreIdempotentSourceFailure() {
    final DropWritableViewProcedure procedure =
        new DropWritableViewProcedure("database1", "view1", "0", false);
    procedure.setTable(new WritableView("view1", "database1", "source1", true));

    final SetTablePropertiesProcedure sourceProcedure =
        new SetTablePropertiesProcedure(
            "database1", "source1", "0_source", Collections.singletonMap("ttl", "1"), false);
    final AtomicBoolean invoked = new AtomicBoolean(false);

    WritableViewUtils.executeForSource(
        procedure,
        sourceProcedure,
        () -> {
          invoked.set(true);
          sourceProcedure.setFailure(
              new ProcedureException(
                  new RuntimeException(
                      new IoTDBException(
                          "Source column does not exist", COLUMN_NOT_EXISTS.getStatusCode()))));
        });

    Assert.assertTrue(invoked.get());
    Assert.assertTrue(sourceProcedure.isFailed());
    Assert.assertFalse(procedure.isFailed());
  }

  @Test
  public void executeForSourceShouldPropagateNonIdempotentSourceFailure() {
    final DropWritableViewProcedure procedure =
        new DropWritableViewProcedure("database1", "view1", "0", false);
    procedure.setTable(new WritableView("view1", "database1", "source1", true));

    final SetTablePropertiesProcedure sourceProcedure =
        new SetTablePropertiesProcedure(
            "database1", "source1", "0_source", Collections.singletonMap("ttl", "1"), false);
    final ProcedureException expectedFailure =
        new ProcedureException(
            new IoTDBException(
                "Source table already exists", TABLE_ALREADY_EXISTS.getStatusCode()));

    WritableViewUtils.executeForSource(
        procedure, sourceProcedure, () -> sourceProcedure.setFailure(expectedFailure));

    Assert.assertTrue(procedure.isFailed());
    Assert.assertSame(expectedFailure, procedure.getFailure());
  }

  @Test
  public void getSourceDatabaseAndTableShouldSkipWhenSourceTableMissing() throws Exception {
    final DropWritableViewProcedure procedure =
        new DropWritableViewProcedure("database1", "view1", "0", false);
    procedure.setTable(new WritableView("view1", "database1", "source1", true));

    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.empty());

    ConfigNode.setInstance(mockedConfigNode);
    try {
      Assert.assertNull(WritableViewUtils.getSourceDatabaseAndTable(procedure, null));
      Assert.assertFalse(procedure.isFailed());
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  @Test
  public void dropWritableViewShouldSkipOriginalResolutionWhenSchemaCascadeDisabled() {
    final DropWritableViewProcedure procedure =
        new DropWritableViewProcedure("database1", "view1", "0", false);
    procedure.setTable(new WritableView("view1", "database1", "source1", false));

    Assert.assertNull(procedure.getOriginalDatabase());
    Assert.assertNull(procedure.getOriginalTable());
  }

  @Test
  public void dropWritableViewShouldResolveOriginalTableWhenSchemaCascadeEnabled()
      throws Exception {
    final DropWritableViewProcedure procedure =
        new DropWritableViewProcedure("database1", "view1", "0", false);
    procedure.setTable(new WritableView("view1", "database1", "source1", true));

    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    final TsTable sourceTable = new TsTable("source1");
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.of(new Pair<>(sourceTable, TableNodeStatus.USING)));

    ConfigNode.setInstance(mockedConfigNode);
    try {
      Assert.assertEquals("database1", procedure.getOriginalDatabase());
      Assert.assertEquals(sourceTable, procedure.getOriginalTable());
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  @Test
  public void dropWritableViewColumnShouldResolveOriginalColumnOnlyWhenCascadeEnabled()
      throws Exception {
    final DropWritableViewColumnProcedure cascadeProcedure =
        new DropWritableViewColumnProcedure("database1", "view1", "0", "viewColumn", false);
    final WritableView cascadeView = new WritableView("view1", "database1", "source1", true);
    final AttributeColumnSchema cascadeViewColumn =
        new AttributeColumnSchema("viewColumn", TSDataType.STRING);
    ViewColumnSchemaUtils.setSourceName(cascadeViewColumn, "sourceColumn");
    cascadeView.addColumnSchema(cascadeViewColumn);
    cascadeProcedure.setTable(cascadeView);

    final DropWritableViewColumnProcedure nonCascadeProcedure =
        new DropWritableViewColumnProcedure("database1", "view1", "0", "viewColumn", false);
    final WritableView nonCascadeView = new WritableView("view1", "database1", "source1", false);
    final AttributeColumnSchema nonCascadeViewColumn =
        new AttributeColumnSchema("viewColumn", TSDataType.STRING);
    ViewColumnSchemaUtils.setSourceName(nonCascadeViewColumn, "sourceColumn");
    nonCascadeView.addColumnSchema(nonCascadeViewColumn);
    nonCascadeProcedure.setTable(nonCascadeView);

    final DropWritableViewColumnProcedure renamedCascadeProcedure =
        new DropWritableViewColumnProcedure("database1", "view1", "0", "renamedViewColumn", false);
    final WritableView renamedCascadeView = new WritableView("view1", "database1", "source1", true);
    renamedCascadeView.addColumnSchema(new AttributeColumnSchema("viewColumn", TSDataType.STRING));
    renamedCascadeView.renameColumnSchema("viewColumn", "renamedViewColumn");
    renamedCascadeProcedure.setTable(renamedCascadeView);

    final ConfigNode originalConfigNode = ConfigNode.getInstance();
    final ConfigNode mockedConfigNode = mock(ConfigNode.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
    when(mockedConfigNode.getConfigManager()).thenReturn(configManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(clusterSchemaManager.getTableAndStatusIfExists("database1", "source1"))
        .thenReturn(Optional.of(new Pair<>(new TsTable("source1"), TableNodeStatus.USING)));

    ConfigNode.setInstance(mockedConfigNode);
    try {
      Assert.assertEquals("sourceColumn", getOriginalColumnName(cascadeProcedure));
      Assert.assertNull(getOriginalColumnName(nonCascadeProcedure));
      Assert.assertEquals("viewColumn", getOriginalColumnName(renamedCascadeProcedure));
    } finally {
      ConfigNode.setInstance(originalConfigNode);
    }
  }

  private void assertAddWritableViewColumnSerializeDeserialize(
      final AddWritableViewColumnProcedure procedure,
      final ProcedureType procedureType,
      final AddWritableViewColumnProcedure deserializedProcedure)
      throws IOException {
    setFieldValue(
        procedure,
        AddWritableViewColumnProcedure.class,
        "originalAddedColumnList",
        Collections.singletonList(new TagColumnSchema("source_id", TSDataType.STRING)));

    final ByteBuffer byteBuffer = serializeAndAssertTypeCode(procedure, procedureType);

    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(procedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(procedure.getTableName(), deserializedProcedure.getTableName());
    Assert.assertEquals(procedure.getQueryId(), deserializedProcedure.getQueryId());
    assertColumnSchemaListEquals(
        getFieldValue(procedure, AddTableColumnProcedure.class, "addedColumnList"),
        getFieldValue(deserializedProcedure, AddTableColumnProcedure.class, "addedColumnList"));
    assertColumnSchemaListEquals(
        getFieldValue(procedure, AddWritableViewColumnProcedure.class, "originalAddedColumnList"),
        getFieldValue(
            deserializedProcedure,
            AddWritableViewColumnProcedure.class,
            "originalAddedColumnList"));
  }

  private void assertSetWritableViewPropertiesSerializeDeserialize(
      final SetWritableViewPropertiesProcedure procedure,
      final ProcedureType procedureType,
      final SetWritableViewPropertiesProcedure deserializedProcedure)
      throws IOException {
    setFieldValue(
        procedure, SetWritableViewPropertiesProcedure.class, "shouldRunSourceProcedure", false);

    final ByteBuffer byteBuffer = serializeAndAssertTypeCode(procedure, procedureType);

    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(procedure, deserializedProcedure);
    Assert.assertEquals(
        Boolean.FALSE,
        getFieldValue(
            deserializedProcedure,
            SetWritableViewPropertiesProcedure.class,
            "shouldRunSourceProcedure"));
  }

  private void assertSerializeDeserialize(
      final Procedure<?> procedure,
      final ProcedureType procedureType,
      final Procedure<?> deserializedProcedure)
      throws IOException {
    final ByteBuffer byteBuffer = serializeAndAssertTypeCode(procedure, procedureType);

    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(procedure, deserializedProcedure);
  }

  private ByteBuffer serializeAndAssertTypeCode(
      final Procedure<?> procedure, final ProcedureType procedureType) throws IOException {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    procedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    Assert.assertEquals(procedureType.getTypeCode(), byteBuffer.getShort());
    return byteBuffer;
  }

  private void assertColumnSchemaListEquals(
      final List<TsTableColumnSchema> expected, final List<TsTableColumnSchema> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      final TsTableColumnSchema expectedSchema = expected.get(i);
      final TsTableColumnSchema actualSchema = actual.get(i);
      Assert.assertEquals(expectedSchema.getClass(), actualSchema.getClass());
      Assert.assertEquals(expectedSchema.getColumnName(), actualSchema.getColumnName());
      Assert.assertEquals(expectedSchema.getDataType(), actualSchema.getDataType());
      Assert.assertEquals(expectedSchema.getColumnCategory(), actualSchema.getColumnCategory());
      Assert.assertEquals(expectedSchema.getProps(), actualSchema.getProps());
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getFieldValue(
      final Object target, final Class<?> ownerClass, final String fieldName) {
    try {
      final Field field = ownerClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T) field.get(target);
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

  private String getOriginalColumnName(final DropWritableViewColumnProcedure procedure) {
    try {
      final java.lang.reflect.Method method =
          AbstractAlterOrDropTableColumnProcedure.class.getDeclaredMethod("getOriginalColumnName");
      method.setAccessible(true);
      return (String) method.invoke(procedure);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
