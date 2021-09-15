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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.Constants;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateSnapshotPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

public class MetaLogApplierTest extends IoTDBTest {

  private Set<Node> nodes = new HashSet<>();

  private TestMetaGroupMember testMetaGroupMember =
      new TestMetaGroupMember() {
        @Override
        public void applyAddNode(AddNodeLog addNodeLog) {
          nodes.add(addNodeLog.getNewNode());
        }

        @Override
        public void applyRemoveNode(RemoveNodeLog removeNodeLog) {
          nodes.remove(removeNodeLog.getRemovedNode());
        }
      };

  private LogApplier applier = new MetaLogApplier(testMetaGroupMember);

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    testMetaGroupMember.stop();
    testMetaGroupMember.closeLogManager();
    super.tearDown();
  }

  @Test
  public void testApplyAddNode() {
    nodes.clear();
    testMetaGroupMember.setCoordinator(new Coordinator());
    testMetaGroupMember.setPartitionTable(TestUtils.getPartitionTable(3));
    Node node = new Node("localhost", 1111, 0, 2222, Constants.RPC_PORT, "localhost");
    AddNodeLog log = new AddNodeLog();
    log.setNewNode(node);
    log.setPartitionTable(TestUtils.seralizePartitionTable);
    applier.apply(log);

    assertTrue(nodes.contains(node));
  }

  @Test
  public void testApplyRemoveNode() {
    nodes.clear();

    Node node = testMetaGroupMember.getThisNode();
    RemoveNodeLog log = new RemoveNodeLog();
    log.setPartitionTable(TestUtils.seralizePartitionTable);
    log.setRemovedNode(node);
    applier.apply(log);

    assertFalse(nodes.contains(node));
  }

  @Test
  public void testApplyMetadataCreation() throws MetadataException {
    PhysicalPlanLog physicalPlanLog = new PhysicalPlanLog();
    SetStorageGroupPlan setStorageGroupPlan =
        new SetStorageGroupPlan(new PartialPath("root.applyMeta"));
    physicalPlanLog.setPlan(setStorageGroupPlan);

    applier.apply(physicalPlanLog);
    assertTrue(IoTDB.metaManager.isPathExist(new PartialPath("root.applyMeta")));

    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.applyMeta" + ".s1"),
            TSDataType.DOUBLE,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null);
    physicalPlanLog.setPlan(createTimeSeriesPlan);
    applier.apply(physicalPlanLog);
    assertTrue(IoTDB.metaManager.isPathExist(new PartialPath("root.applyMeta.s1")));
    assertEquals(
        TSDataType.DOUBLE,
        IoTDB.metaManager.getSeriesType(new PartialPath("root" + ".applyMeta.s1")));
  }

  @Test
  public void testApplyCreateSnapshot() {
    CreateSnapshotPlan createSnapshotPlan = new CreateSnapshotPlan();
    PhysicalPlanLog physicalPlanLog = new PhysicalPlanLog(createSnapshotPlan);
    applier.apply(physicalPlanLog);
    assertNull(physicalPlanLog.getException());
  }
}
