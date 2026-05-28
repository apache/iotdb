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

package org.apache.iotdb.confignode.manager.pipe.receiver.protocol;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewColumnCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;
import com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable.CreateWritableViewProcedure;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class WritableViewPipeReplayTest {

  @Test
  public void testBuildCreateWritableViewProcedure() {
    final WritableView view = new WritableView("view_table", "src_db", "src_table", true);

    final Procedure<ConfigNodeProcedureEnv> procedure =
        IoTDBConfigNodeReceiver.buildCreateTableOrViewProcedure("view_db", view, true);

    Assert.assertEquals(
        ProcedureType.CREATE_WRITABLE_VIEW_PROCEDURE,
        IoTDBConfigNodeReceiver.getCreateTableOrViewProcedureType(view));
    Assert.assertTrue(procedure instanceof CreateWritableViewProcedure);
    assertCreateWritableViewProcedureEquals(
        new CreateWritableViewProcedure("view_db", view, false, true),
        (CreateWritableViewProcedure) procedure);
  }

  @Test
  public void testGetSetWritableViewPropertiesPlanDropsSchemaCascadeForOriginalTable() {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, "1");
    properties.put(WritableView.SCHEMA_CASCADE, "true");
    final SetWritableViewPropertiesPlan plan =
        new SetWritableViewPropertiesPlan(
            "view_db", "view_table", properties, "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getSetWritableViewPropertiesPlan(plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof SetTablePropertiesPlan);
    final SetTablePropertiesPlan replayPlan = (SetTablePropertiesPlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("1", replayPlan.getProperties().get(TsTable.TTL_PROPERTY));
    Assert.assertFalse(replayPlan.getProperties().containsKey(WritableView.SCHEMA_CASCADE));
    Assert.assertTrue(plan.getProperties().containsKey(WritableView.SCHEMA_CASCADE));
  }

  @Test
  public void testBuildOriginalTablePropertiesProcedureDropsSchemaCascade() {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, "1");
    properties.put(WritableView.SCHEMA_CASCADE, "true");
    final SetWritableViewPropertiesPlan plan =
        new SetWritableViewPropertiesPlan(
            "view_db", "view_table", properties, "src_db", "src_table");

    final SetTablePropertiesProcedure procedure =
        IoTDBConfigNodeReceiver.buildOriginalTablePropertiesProcedure(plan, "query_1", true);

    final Map<String, String> expectedProperties = new HashMap<>();
    expectedProperties.put(TsTable.TTL_PROPERTY, "1");
    Assert.assertEquals(
        new SetTablePropertiesProcedure("src_db", "src_table", "query_1", expectedProperties, true),
        procedure);
  }

  @Test
  public void testGetSetWritableViewPropertiesPlanKeepsCascadeViewOnlyWhenOriginalInvisible() {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, "1");
    properties.put(WritableView.SCHEMA_CASCADE, "true");
    final SetWritableViewPropertiesPlan plan =
        new SetWritableViewPropertiesPlan(
            "view_db", "view_table", properties, "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getSetWritableViewPropertiesPlan(plan, true, false);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof SetWritableViewPropertiesPlan);
    final SetWritableViewPropertiesPlan replayPlan = (SetWritableViewPropertiesPlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertNull(replayPlan.getOriginalDatabase());
    Assert.assertNull(replayPlan.getOriginalTableName());
    Assert.assertEquals("true", replayPlan.getProperties().get(WritableView.SCHEMA_CASCADE));
  }

  @Test
  public void testGetSetWritableViewCommentPlanUsesOriginalTableWhenViewInvisible() {
    final SetWritableViewCommentPlan plan =
        new SetWritableViewCommentPlan("view_db", "view_table", "comment", "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getSetWritableViewCommentPlan(plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof SetTableCommentPlan);
    final SetTableCommentPlan replayPlan = (SetTableCommentPlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("comment", replayPlan.getComment());
  }

  @Test
  public void testBuildOriginalTableColumnCommentPlanUsesOriginalColumnName() {
    final SetWritableViewColumnCommentPlan plan =
        new SetWritableViewColumnCommentPlan(
            "view_db", "view_table", "view_col", "comment", "src_db", "src_table", "src_col");

    final SetTableColumnCommentPlan replayPlan =
        IoTDBConfigNodeReceiver.buildOriginalTableColumnCommentPlan(plan);

    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("src_col", replayPlan.getColumnName());
    Assert.assertEquals("comment", replayPlan.getComment());
  }

  @Test
  public void testGetAddWritableViewColumnPlanUsesViewOnlyPlanWhenOriginalInvisible() {
    final AddWritableViewColumnPlan plan =
        new AddWritableViewColumnPlan(
            "view_db",
            "view_table",
            Collections.singletonList(new FieldColumnSchema("view_col", TSDataType.INT32)),
            false,
            "src_db",
            "src_table",
            Collections.singletonList(new FieldColumnSchema("src_col", TSDataType.INT32)));

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getAddWritableViewColumnPlan(plan, true, false);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof AddWritableViewColumnPlan);
    final AddWritableViewColumnPlan replayPlan = (AddWritableViewColumnPlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertNull(replayPlan.getOriginalDatabase());
    Assert.assertNull(replayPlan.getOriginalTableName());
    Assert.assertNull(replayPlan.getOriginalColumnSchemaList());
    Assert.assertEquals("view_col", replayPlan.getColumnSchemaList().get(0).getColumnName());
  }

  @Test
  public void testGetAddWritableViewColumnPlanUsesOriginalPlanWhenViewInvisible() {
    final AddWritableViewColumnPlan plan =
        new AddWritableViewColumnPlan(
            "view_db",
            "view_table",
            Collections.singletonList(new FieldColumnSchema("view_col", TSDataType.INT32)),
            false,
            "src_db",
            "src_table",
            Collections.singletonList(new FieldColumnSchema("src_col", TSDataType.INT32)));

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getAddWritableViewColumnPlan(plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof AddTableColumnPlan);
    final AddTableColumnPlan replayPlan = (AddTableColumnPlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("src_col", replayPlan.getColumnSchemaList().get(0).getColumnName());
  }

  @Test
  public void testGetCommitDeleteWritableViewColumnPlanUsesViewOnlyPlanWhenOriginalInvisible() {
    final CommitDeleteWritableViewColumnPlan plan =
        new CommitDeleteWritableViewColumnPlan(
            "view_db", "view_table", "view_col", "src_db", "src_table", "src_col");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewColumnPlan(
            plan, true, false);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof CommitDeleteWritableViewColumnPlan);
    final CommitDeleteWritableViewColumnPlan replayPlan =
        (CommitDeleteWritableViewColumnPlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertEquals("view_col", replayPlan.getColumnName());
    Assert.assertNull(replayPlan.getOriginalDatabase());
    Assert.assertNull(replayPlan.getOriginalTableName());
    Assert.assertNull(replayPlan.getOriginalColumnName());
  }

  @Test
  public void testGetCommitDeleteWritableViewColumnPlanUsesOriginalPlanWhenViewInvisible() {
    final CommitDeleteWritableViewColumnPlan plan =
        new CommitDeleteWritableViewColumnPlan(
            "view_db", "view_table", "view_col", "src_db", "src_table", "src_col");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewColumnPlan(
            plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof CommitDeleteColumnPlan);
    final CommitDeleteColumnPlan replayPlan = (CommitDeleteColumnPlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("src_col", replayPlan.getColumnName());
  }

  @Test
  public void testGetCommitDeleteWritableViewPlanUsesViewOnlyPlanWhenOriginalInvisible() {
    final CommitDeleteWritableViewPlan plan =
        new CommitDeleteWritableViewPlan("view_db", "view_table", "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewPlan(plan, true, false);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof CommitDeleteWritableViewPlan);
    final CommitDeleteWritableViewPlan replayPlan = (CommitDeleteWritableViewPlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertNull(replayPlan.getOriginalDatabase());
    Assert.assertNull(replayPlan.getOriginalTableName());
  }

  @Test
  public void testGetCommitDeleteWritableViewPlanKeepsOriginalWhenBothVisible() {
    final CommitDeleteWritableViewPlan plan =
        new CommitDeleteWritableViewPlan("view_db", "view_table", "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewPlan(plan, true, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof CommitDeleteWritableViewPlan);
    final CommitDeleteWritableViewPlan replayPlan = (CommitDeleteWritableViewPlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertEquals("src_db", replayPlan.getOriginalDatabase());
    Assert.assertEquals("src_table", replayPlan.getOriginalTableName());
  }

  @Test
  public void testGetCommitDeleteWritableViewPlanUsesOriginalPlanWhenViewInvisible() {
    final CommitDeleteWritableViewPlan plan =
        new CommitDeleteWritableViewPlan("view_db", "view_table", "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewPlan(plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof CommitDeleteTablePlan);
    final CommitDeleteTablePlan replayPlan = (CommitDeleteTablePlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
  }

  @Test
  public void testGetCommitDeleteWritableViewPlanSkipsPlanWhenBothInvisible() {
    final CommitDeleteWritableViewPlan plan =
        new CommitDeleteWritableViewPlan("view_db", "view_table", "src_db", "src_table");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewPlan(plan, false, false);

    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void testGetSetWritableViewColumnCommentPlanUsesViewOnlyPlanWhenOriginalInvisible() {
    final SetWritableViewColumnCommentPlan plan =
        new SetWritableViewColumnCommentPlan(
            "view_db", "view_table", "view_col", "comment", "src_db", "src_table", "src_col");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getSetWritableViewColumnCommentPlan(plan, true, false);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof SetWritableViewColumnCommentPlan);
    final SetWritableViewColumnCommentPlan replayPlan =
        (SetWritableViewColumnCommentPlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertEquals("view_col", replayPlan.getColumnName());
    Assert.assertNull(replayPlan.getOriginalDatabase());
    Assert.assertNull(replayPlan.getOriginalTableName());
    Assert.assertNull(replayPlan.getOriginalColumnName());
  }

  @Test
  public void testGetSetWritableViewColumnCommentPlanUsesOriginalPlanWhenViewInvisible() {
    final SetWritableViewColumnCommentPlan plan =
        new SetWritableViewColumnCommentPlan(
            "view_db", "view_table", "view_col", "comment", "src_db", "src_table", "src_col");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getSetWritableViewColumnCommentPlan(plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof SetTableColumnCommentPlan);
    final SetTableColumnCommentPlan replayPlan = (SetTableColumnCommentPlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("src_col", replayPlan.getColumnName());
    Assert.assertEquals("comment", replayPlan.getComment());
  }

  @Test
  public void testGetAlterWritableViewColumnDataTypePlanUsesViewOnlyPlanWhenOriginalInvisible() {
    final AlterWritableViewColumnDataTypePlan plan =
        new AlterWritableViewColumnDataTypePlan(
            "view_db",
            "view_table",
            "view_col",
            TSDataType.INT64,
            "src_db",
            "src_table",
            "src_col");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getAlterWritableViewColumnDataTypePlan(
            plan, true, false);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof AlterWritableViewColumnDataTypePlan);
    final AlterWritableViewColumnDataTypePlan replayPlan =
        (AlterWritableViewColumnDataTypePlan) result.get();
    Assert.assertEquals("view_db", replayPlan.getDatabase());
    Assert.assertEquals("view_table", replayPlan.getTableName());
    Assert.assertEquals("view_col", replayPlan.getColumnName());
    Assert.assertEquals(TSDataType.INT64, replayPlan.getNewType());
    Assert.assertNull(replayPlan.getOriginalDatabase());
    Assert.assertNull(replayPlan.getOriginalTableName());
    Assert.assertNull(replayPlan.getOriginalColumnName());
  }

  @Test
  public void testGetAlterWritableViewColumnDataTypePlanUsesOriginalPlanWhenViewInvisible() {
    final AlterWritableViewColumnDataTypePlan plan =
        new AlterWritableViewColumnDataTypePlan(
            "view_db",
            "view_table",
            "view_col",
            TSDataType.INT64,
            "src_db",
            "src_table",
            "src_col");

    final Optional<ConfigPhysicalPlan> result =
        PipeConfigTablePrivilegeParseVisitor.getAlterWritableViewColumnDataTypePlan(
            plan, false, true);

    Assert.assertTrue(result.isPresent());
    Assert.assertTrue(result.get() instanceof AlterColumnDataTypePlan);
    final AlterColumnDataTypePlan replayPlan = (AlterColumnDataTypePlan) result.get();
    Assert.assertEquals("src_db", replayPlan.getDatabase());
    Assert.assertEquals("src_table", replayPlan.getTableName());
    Assert.assertEquals("src_col", replayPlan.getColumnName());
    Assert.assertEquals(TSDataType.INT64, replayPlan.getNewType());
  }

  private void assertCreateWritableViewProcedureEquals(
      final CreateWritableViewProcedure expected, final CreateWritableViewProcedure actual) {
    Assert.assertEquals(expected.getDatabase(), actual.getDatabase());
    Assert.assertEquals(
        getFieldValue(expected, StateMachineProcedure.class, "isGeneratedByPipe"),
        getFieldValue(actual, StateMachineProcedure.class, "isGeneratedByPipe"));
    Assert.assertEquals(
        getFieldValue(expected, CreateTableViewProcedure.class, "replace"),
        getFieldValue(actual, CreateTableViewProcedure.class, "replace"));
    Assert.assertEquals(
        getFieldValue(expected, CreateWritableViewProcedure.class, "viewColumnCommentMap"),
        getFieldValue(actual, CreateWritableViewProcedure.class, "viewColumnCommentMap"));
    assertNullableTsTableEquals(
        (TsTable) getFieldValue(expected, CreateTableViewProcedure.class, "oldView"),
        (TsTable) getFieldValue(actual, CreateTableViewProcedure.class, "oldView"));
    Assert.assertEquals(
        getFieldValue(expected, CreateTableViewProcedure.class, "oldStatus"),
        getFieldValue(actual, CreateTableViewProcedure.class, "oldStatus"));
    assertNullableTsTableEquals(expected.getOriginalTable(), actual.getOriginalTable());
    assertWritableViewEquals((WritableView) expected.getTable(), (WritableView) actual.getTable());
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
}
