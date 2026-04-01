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

package org.apache.iotdb.confignode.procedure.impl.pipe.receiver;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterEncodingCompressorProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DeleteDevicesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.AddViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.SetViewPropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.AuthOperationProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PipeEnrichedProcedureTest {
  @Test
  public void deleteDatabaseTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    DeleteDatabaseProcedure p1 = new DeleteDatabaseProcedure(new TDatabaseSchema("root.sg"), true);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      DeleteDatabaseProcedure p2 =
          (DeleteDatabaseProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);

    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void deleteTimeseriesTest() throws IllegalPathException, IOException {
    String queryId = "1";
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg1.**"));
    patternTree.appendPathPattern(new PartialPath("root.sg2.*.s1"));
    patternTree.constructTree();
    DeleteTimeSeriesProcedure deleteTimeSeriesProcedure =
        new DeleteTimeSeriesProcedure(queryId, patternTree, true, false);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    deleteTimeSeriesProcedure.serialize(dataOutputStream);

    DeleteTimeSeriesProcedure deserializedProcedure =
        (DeleteTimeSeriesProcedure)
            ProcedureFactory.getInstance()
                .create(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));

    Assert.assertEquals(deleteTimeSeriesProcedure, deserializedProcedure);
  }

  @Test
  public void deactivateTemplateTest() throws IllegalPathException, IOException {
    String queryId = "1";
    Map<PartialPath, List<Template>> templateSetInfo = new HashMap<>();
    Template t1 = new Template();
    t1.setId(0);
    t1.setName("t1");
    t1.addMeasurements(
        new String[] {"s1", "s2"},
        new TSDataType[] {TSDataType.INT32, TSDataType.FLOAT},
        new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.BITMAP},
        new CompressionType[] {CompressionType.UNCOMPRESSED, CompressionType.GZIP});

    Template t2 = new Template();
    t2.setId(0);
    t2.setName("t2");
    t2.addMeasurements(
        new String[] {"s3", "s4"},
        new TSDataType[] {TSDataType.FLOAT, TSDataType.INT32},
        new TSEncoding[] {TSEncoding.BITMAP, TSEncoding.PLAIN},
        new CompressionType[] {CompressionType.GZIP, CompressionType.UNCOMPRESSED});

    templateSetInfo.put(new PartialPath("root.sg1.**"), Arrays.asList(t1, t2));
    templateSetInfo.put(new PartialPath("root.sg2.**"), Arrays.asList(t2, t1));

    DeactivateTemplateProcedure deactivateTemplateProcedure =
        new DeactivateTemplateProcedure(queryId, templateSetInfo, true);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    deactivateTemplateProcedure.serialize(dataOutputStream);

    DeactivateTemplateProcedure deserializedProcedure =
        (DeactivateTemplateProcedure)
            ProcedureFactory.getInstance()
                .create(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));

    Assert.assertEquals(deactivateTemplateProcedure, deserializedProcedure);
  }

  @Test
  public void unsetTemplateTest() throws IllegalPathException, IOException {
    String queryId = "1";
    Template template = new Template();
    template.setId(0);
    template.setName("t1");
    template.addMeasurements(
        new String[] {"s1", "s2"},
        new TSDataType[] {TSDataType.INT32, TSDataType.FLOAT},
        new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.BITMAP},
        new CompressionType[] {CompressionType.UNCOMPRESSED, CompressionType.GZIP});
    PartialPath path = new PartialPath("root.sg");
    UnsetTemplateProcedure unsetTemplateProcedure =
        new UnsetTemplateProcedure(queryId, template, path, true);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    unsetTemplateProcedure.serialize(dataOutputStream);

    UnsetTemplateProcedure deserializedProcedure =
        (UnsetTemplateProcedure)
            ProcedureFactory.getInstance()
                .create(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));

    Assert.assertEquals(unsetTemplateProcedure, deserializedProcedure);
  }

  @Test
  public void setTemplateTest() throws IOException {
    SetTemplateProcedure setTemplateProcedure =
        new SetTemplateProcedure("1", "t1", "root.sg", true);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    setTemplateProcedure.serialize(dataOutputStream);

    SetTemplateProcedure deserializedProcedure =
        (SetTemplateProcedure)
            ProcedureFactory.getInstance()
                .create(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));

    Assert.assertEquals(setTemplateProcedure, deserializedProcedure);
  }

  @Test
  public void alterLogicalViewTest() throws IllegalPathException, IOException {
    AlterLogicalViewProcedure alterLogicalViewProcedure =
        new AlterLogicalViewProcedure(
            "1",
            new HashMap<PartialPath, ViewExpression>() {
              {
                put(
                    new PartialPath("root.sg"),
                    new ConstantViewOperand(TSDataType.BOOLEAN, "true"));
              }
            },
            true);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    alterLogicalViewProcedure.serialize(dataOutputStream);

    AlterLogicalViewProcedure deserializedProcedure =
        (AlterLogicalViewProcedure)
            ProcedureFactory.getInstance()
                .create(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));

    Assert.assertEquals(alterLogicalViewProcedure.getQueryId(), deserializedProcedure.getQueryId());
    // Currently skip the "equals" method since "equals" of ViewExpression is not implemented
  }

  @Test
  public void deleteLogicalViewTest() throws IllegalPathException, IOException {
    PathPatternTree tree = new PathPatternTree();
    tree.appendFullPath(new PartialPath("root.a.b"));
    tree.appendFullPath(new PartialPath("root.a.c"));
    DeleteLogicalViewProcedure deleteLogicalViewProcedure =
        new DeleteLogicalViewProcedure("1", tree, true);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    deleteLogicalViewProcedure.serialize(dataOutputStream);

    DeleteLogicalViewProcedure deserializedProcedure =
        (DeleteLogicalViewProcedure)
            ProcedureFactory.getInstance()
                .create(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));

    Assert.assertEquals(deleteLogicalViewProcedure, deserializedProcedure);
  }

  @Test
  public void createTriggerTest() throws IllegalPathException {

    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    TriggerInformation triggerInformation =
        new TriggerInformation(
            new PartialPath("root.test.**"),
            "test",
            "test.class",
            true,
            "test.jar",
            null,
            TriggerEvent.AFTER_INSERT,
            TTriggerState.INACTIVE,
            false,
            null,
            FailureStrategy.OPTIMISTIC,
            "testMD5test");
    CreateTriggerProcedure p1 =
        new CreateTriggerProcedure(triggerInformation, new Binary(new byte[] {1, 2, 3}), true);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      CreateTriggerProcedure p2 =
          (CreateTriggerProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);
      assertEquals(p1.getJarFile(), p2.getJarFile());

    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void dropTriggerTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    DropTriggerProcedure p1 = new DropTriggerProcedure("test", true);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      DropTriggerProcedure p2 =
          (DropTriggerProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void authOperationTest() throws AuthException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    AuthOperationProcedure p1 =
        new AuthOperationProcedure(
            new AuthorTreePlan(
                ConfigPhysicalPlanType.DropRole,
                "",
                "test",
                "",
                "",
                Collections.emptySet(),
                false,
                Collections.emptyList()),
            Collections.emptyList(),
            true);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      AuthOperationProcedure p2 =
          (AuthOperationProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void setTTLTest() throws IOException, IllegalPathException {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    // test1
    PartialPath path = new PartialPath("root.test.sg1.group1.group1.**");
    SetTTLPlan setTTLPlan = new SetTTLPlan(Arrays.asList(path.getNodes()), 1928300234200L);
    SetTTLProcedure proc = new SetTTLProcedure(setTTLPlan, true);

    proc.serialize(outputStream);
    ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    SetTTLProcedure proc2 = (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    assertEquals(proc, proc2);
    buffer.clear();
    byteArrayOutputStream.reset();

    // test2
    path = new PartialPath("root.**");
    setTTLPlan = new SetTTLPlan(Arrays.asList(path.getNodes()), -1);
    proc = new SetTTLProcedure(setTTLPlan, true);

    proc.serialize(outputStream);
    buffer = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    proc2 = (SetTTLProcedure) ProcedureFactory.getInstance().create(buffer);
    assertEquals(proc, proc2);
    buffer.clear();
    byteArrayOutputStream.reset();
  }

  @Test
  public void createTableTest() throws IOException {
    final TsTable table = new TsTable("table1");
    table.addColumnSchema(new TagColumnSchema("Id", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("Attr", TSDataType.STRING));
    table.addColumnSchema(
        new FieldColumnSchema(
            "Measurement", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
    final CreateTableProcedure createTableProcedure =
        new CreateTableProcedure("database1", table, true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    createTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_CREATE_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final CreateTableProcedure deserializedProcedure = new CreateTableProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(createTableProcedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(
        createTableProcedure.getTable().getTableName(),
        deserializedProcedure.getTable().getTableName());
    Assert.assertEquals(
        createTableProcedure.getTable().getColumnNum(),
        deserializedProcedure.getTable().getColumnNum());
    Assert.assertEquals(
        createTableProcedure.getTable().getTagNum(), deserializedProcedure.getTable().getTagNum());
  }

  @Test
  public void addTableColumnTest() throws IOException {
    final AddTableColumnProcedure addTableColumnProcedure =
        new AddTableColumnProcedure(
            "database1",
            "table1",
            "0",
            Collections.singletonList(new TagColumnSchema("Id", TSDataType.STRING)),
            true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    addTableColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_ADD_TABLE_COLUMN_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final AddTableColumnProcedure deserializedProcedure = new AddTableColumnProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(addTableColumnProcedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(
        addTableColumnProcedure.getTableName(), deserializedProcedure.getTableName());
  }

  @Test
  public void setTablePropertiesTest() throws IOException {
    final SetTablePropertiesProcedure setTablePropertiesProcedure =
        new SetTablePropertiesProcedure(
            "database1",
            "table1",
            "0",
            new HashMap<String, String>() {
              {
                put("prop1", "value1");
                put("ttl", null);
              }
            },
            true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    setTablePropertiesProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_SET_TABLE_PROPERTIES_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final SetTablePropertiesProcedure deserializedProcedure = new SetTablePropertiesProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(setTablePropertiesProcedure, deserializedProcedure);
  }

  @Test
  public void renameTableColumnTest() throws IOException {
    final RenameTableColumnProcedure renameTableColumnProcedure =
        new RenameTableColumnProcedure("database1", "table1", "0", "oldName", "newName", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    renameTableColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_RENAME_TABLE_COLUMN_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final RenameTableColumnProcedure deserializedProcedure = new RenameTableColumnProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(renameTableColumnProcedure, deserializedProcedure);
  }

  @Test
  public void dropTableTest() throws IOException {
    final DropTableProcedure dropTableProcedure =
        new DropTableProcedure("database1", "table1", "0", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_DROP_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final DropTableProcedure deserializedProcedure = new DropTableProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropTableProcedure, deserializedProcedure);
  }

  @Test
  public void dropTableColumnTest() throws IOException {
    final DropTableColumnProcedure dropTableColumnProcedure =
        new DropTableColumnProcedure("database1", "table1", "0", "columnName", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropTableColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_DROP_TABLE_COLUMN_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final DropTableColumnProcedure deserializedProcedure = new DropTableColumnProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropTableColumnProcedure, deserializedProcedure);
  }

  @Test
  public void deleteDevicesProcedureTest() throws IOException {
    final DeleteDevicesProcedure deleteDevicesProcedure =
        new DeleteDevicesProcedure(
            "database1",
            "table1",
            "0",
            new byte[] {0, 1, 2},
            new byte[] {0, 1, 2},
            new byte[] {0, 1, 2},
            true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    deleteDevicesProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_DELETE_DEVICES_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final DeleteDevicesProcedure deserializedProcedure = new DeleteDevicesProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(deleteDevicesProcedure, deserializedProcedure);
  }

  @Test
  public void renameTableTest() throws IOException {
    final RenameTableProcedure renameTableProcedure =
        new RenameTableProcedure("database1", "table1", "0", "newName", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    renameTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_RENAME_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final RenameTableProcedure deserializedProcedure = new RenameTableProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(renameTableProcedure, deserializedProcedure);
  }

  @Test
  public void createTableViewTest() throws IOException {
    final TsTable table = new TsTable("table1");
    table.addColumnSchema(new TagColumnSchema("Id", TSDataType.STRING));
    table.addColumnSchema(
        new FieldColumnSchema(
            "Measurement", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
    final CreateTableViewProcedure createTableViewProcedure =
        new CreateTableViewProcedure("database1", table, false, true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    createTableViewProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_CREATE_TABLE_VIEW_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final CreateTableViewProcedure deserializedProcedure = new CreateTableViewProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(
        createTableViewProcedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(
        createTableViewProcedure.getTable().getTableName(),
        deserializedProcedure.getTable().getTableName());
    Assert.assertEquals(
        createTableViewProcedure.getTable().getColumnNum(),
        deserializedProcedure.getTable().getColumnNum());
    Assert.assertEquals(
        createTableViewProcedure.getTable().getTagNum(),
        deserializedProcedure.getTable().getTagNum());
  }

  @Test
  public void addViewColumnTest() throws IOException {
    final AddViewColumnProcedure addViewColumnProcedure =
        new AddViewColumnProcedure(
            "database1",
            "table1",
            "0",
            Collections.singletonList(new TagColumnSchema("Id", TSDataType.STRING)),
            true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    addViewColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_ADD_VIEW_COLUMN_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final AddViewColumnProcedure deserializedProcedure = new AddViewColumnProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(addViewColumnProcedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(
        addViewColumnProcedure.getTableName(), deserializedProcedure.getTableName());
  }

  @Test
  public void dropViewColumnTest() throws IOException {
    final DropViewColumnProcedure dropViewColumnProcedure =
        new DropViewColumnProcedure("database1", "table1", "0", "columnName", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropViewColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_DROP_VIEW_COLUMN_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final DropViewColumnProcedure deserializedProcedure = new DropViewColumnProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropViewColumnProcedure, deserializedProcedure);
  }

  @Test
  public void dropViewTest() throws IOException {
    final DropViewProcedure dropViewProcedure =
        new DropViewProcedure("database1", "table1", "0", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dropViewProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_DROP_VIEW_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final DropViewProcedure deserializedProcedure = new DropViewProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(dropViewProcedure, deserializedProcedure);
  }

  @Test
  public void renameViewColumnTest() throws IOException {
    final RenameViewColumnProcedure renameViewColumnProcedure =
        new RenameViewColumnProcedure("database1", "table1", "0", "oldName", "newName", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    renameViewColumnProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_RENAME_VIEW_COLUMN_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final RenameViewColumnProcedure deserializedProcedure = new RenameViewColumnProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(renameViewColumnProcedure, deserializedProcedure);
  }

  @Test
  public void renameViewTest() throws IOException {
    final RenameViewProcedure renameViewProcedure =
        new RenameViewProcedure("database1", "table1", "0", "newName", true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    renameViewProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_RENAME_VIEW_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final RenameViewProcedure deserializedProcedure = new RenameViewProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(renameViewProcedure, deserializedProcedure);
  }

  @Test
  public void setViewPropertiesTest() throws IOException {
    final SetViewPropertiesProcedure setViewPropertiesProcedure =
        new SetViewPropertiesProcedure(
            "database1",
            "table1",
            "0",
            new HashMap<String, String>() {
              {
                put("prop1", "value1");
                put("ttl", null);
              }
            },
            true);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    setViewPropertiesProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.PIPE_ENRICHED_SET_VIEW_PROPERTIES_PROCEDURE.getTypeCode(),
        byteBuffer.getShort());

    final SetViewPropertiesProcedure deserializedProcedure = new SetViewPropertiesProcedure(true);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(setViewPropertiesProcedure, deserializedProcedure);
  }

  @Test
  public void alterEncodingCompressorTest() throws IllegalPathException, IOException {
    final String queryId = "1";
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg1.**"));
    patternTree.appendPathPattern(new PartialPath("root.sg2.*.s1"));
    patternTree.constructTree();
    final AlterEncodingCompressorProcedure alterEncodingCompressorProcedure =
        new AlterEncodingCompressorProcedure(
            false, queryId, patternTree, false, (byte) 0, (byte) 0, false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    alterEncodingCompressorProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.ALTER_ENCODING_COMPRESSOR_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final AlterEncodingCompressorProcedure deserializedProcedure =
        new AlterEncodingCompressorProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(alterEncodingCompressorProcedure, deserializedProcedure);
  }
}
