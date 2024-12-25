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
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.AuthOperationProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.db.schemaengine.template.Template;
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
        new DeleteTimeSeriesProcedure(queryId, patternTree, true);

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
            new AuthorPlan(
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
}
