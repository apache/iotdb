/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.applier;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Test;

public class MetaLogApplierTest extends IoTDBTest {

  private Set<Node> nodes = new HashSet<>();

  private TestMetaGroupMember testMetaGroupMember = new TestMetaGroupMember() {
    @Override
    public void applyAddNode(Node newNode) {
      nodes.add(newNode);
    }
  };

  private LogApplier applier = new MetaLogApplier(testMetaGroupMember);

  @Test
  public void testApplyAddNode() throws QueryProcessException {
    nodes.clear();

    Node node = new Node("localhost", 1111, 0, 2222);
    AddNodeLog log = new AddNodeLog();
    log.setNewNode(node);
    applier.apply(log);

    assertTrue(nodes.contains(node));
  }

  @Test
  public void testApplyMetadataCreation() throws QueryProcessException, MetadataException {
    PhysicalPlanLog physicalPlanLog = new PhysicalPlanLog();
    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan(new Path("root.applyMeta"));
    physicalPlanLog.setPlan(setStorageGroupPlan);

    applier.apply(physicalPlanLog);
    assertTrue(MManager.getInstance().pathExist("root.applyMeta"));

    CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(new Path("root.applyMeta"
        + ".s1"), TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY,
        Collections.emptyMap());
    physicalPlanLog.setPlan(createTimeSeriesPlan);
    applier.apply(physicalPlanLog);
    assertTrue(MManager.getInstance().pathExist("root.applyMeta.s1"));
    assertEquals(TSDataType.DOUBLE, MManager.getInstance().getSeriesType("root.applyMeta.s1"));
  }

  @Test
  public void testApplyCloseFile() throws StorageEngineException, QueryProcessException {
    StorageGroupProcessor storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    assertFalse(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());

    CloseFileLog closeFileLog = new CloseFileLog(TestUtils.getTestSg(0), true);
    applier.apply(closeFileLog);
    assertTrue(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());
  }
}