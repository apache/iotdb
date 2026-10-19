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

package org.apache.iotdb.db.pipe.sink.protocol.logicalbackup;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskTemporaryRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalBackupSinkTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testTemporaryValidationDoesNotCreateBackupStreams() throws Exception {
    final Path directory = temporaryFolder.getRoot().toPath().resolve("backup");
    final PipeParameters parameters =
        new PipeParameters(
            Map.of(
                "sink.dir",
                directory.toString(),
                "sink.backup-id",
                "backup",
                "sink.resume",
                "append"));
    try (final LogicalBackupSink sink = new LogicalBackupSink("data")) {
      sink.validate(new PipeParameterValidator(parameters));
      sink.customize(
          parameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskTemporaryRuntimeEnvironment("pipe")));
      sink.handshake();
    }
    Assert.assertFalse(Files.exists(directory));
  }

  @Test
  public void testDeleteTimeFilterPreservesPathsAndNeverExpandsTheRange() throws Exception {
    final DeleteDataNode original =
        new DeleteDataNode(
            new PlanNodeId("deletion"),
            Collections.singletonList(new MeasurementPath("root.backup.d.s1")),
            10L,
            20L);
    final DeleteDataNode bounded =
        (DeleteDataNode) LogicalBackupSink.boundedDeletion(original, 12L, 15L).get();
    Assert.assertEquals(original.getPathList(), bounded.getPathList());
    Assert.assertEquals(12L, bounded.getDeleteStartTime());
    Assert.assertEquals(15L, bounded.getDeleteEndTime());
    Assert.assertEquals(10L, original.getDeleteStartTime());
    Assert.assertEquals(20L, original.getDeleteEndTime());
    Assert.assertFalse(LogicalBackupSink.boundedDeletion(original, 21L, 30L).isPresent());
  }

  @Test
  public void testInsertNodeIsFilteredBeforeItIsArchived() throws Exception {
    final InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId("backup-test"),
            new PartialPath("root.backup.d"),
            true,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT32},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32)
            },
            new long[] {1, 2, 3},
            null,
            new Object[] {new int[] {10, 20, 30}, new int[] {40, 50, 60}},
            3);
    final PipeInsertNodeTabletInsertionEvent event =
        new PipeInsertNodeTabletInsertionEvent(
            false,
            "root.backup",
            node,
            null,
            0,
            null,
            new IoTDBTreePattern("root.backup.d.s1"),
            null,
            null,
            null,
            null,
            true,
            2L,
            2L);
    final List<TPipeTransferReq> requests = new LogicalBackupSink("data").requests(event);
    Assert.assertEquals(1, requests.size());
    final InsertTabletStatement restored =
        PipeTransferTabletRawReqV2.fromTPipeTransferReq(requests.get(0)).constructStatement();
    Assert.assertArrayEquals(new String[] {"s1"}, restored.getMeasurements());
    Assert.assertArrayEquals(new long[] {2}, restored.getTimes());
    Assert.assertArrayEquals(new int[] {20}, (int[]) restored.getColumns()[0]);
    Assert.assertTrue(restored.isAligned());
  }

  @Test
  public void testDataRegionRequiresExplicitAppendResumePolicy() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSinkConstant.SINK_LOGICAL_BACKUP_DIR_KEY, "backup");

    assertInvalid(new LogicalBackupSink("data"), attributes);

    attributes.put(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_FAIL_IF_EXISTS);
    assertInvalid(new LogicalBackupSink("data"), attributes);

    attributes.put(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND);
    new LogicalBackupSink("data")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));

    attributes.remove(PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY);
    attributes.put(
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND);
    new LogicalBackupSink("data")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));
  }

  @Test
  public void testSchemaRegionKeepsDefaultResumePolicy() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSinkConstant.SINK_LOGICAL_BACKUP_DIR_KEY, "backup");

    new LogicalBackupSink("schema")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));
  }

  @Test
  public void testConfigRegionRequiresExplicitAppendResumePolicy() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSinkConstant.SINK_LOGICAL_BACKUP_DIR_KEY, "backup");

    assertInvalid(new LogicalBackupSink("config"), attributes);

    attributes.put(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND);
    new LogicalBackupSink("config")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));
  }

  private static void assertInvalid(
      final LogicalBackupSink sink, final Map<String, String> attributes) throws Exception {
    try {
      sink.validate(new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));
      Assert.fail();
    } catch (final PipeParameterNotValidException expected) {
      Assert.assertTrue(expected.getMessage().contains("append"));
    }
  }
}
