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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileObjectPieceNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(StorageEngine.class)
public class LoadTsFileDispatcherImplTest {

  @Test
  public void testDispatchLocallyObjectPieceNodeSuccess() throws Exception {
    final StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    PowerMockito.mockStatic(StorageEngine.class);
    PowerMockito.when(StorageEngine.getInstance()).thenReturn(storageEngine);

    final LoadTsFileDispatcherImpl dispatcher = new LoadTsFileDispatcherImpl(null, false);
    dispatcher.setUuid("test-uuid");

    final LoadTsFileObjectPieceNode objectPieceNode = createObjectPieceNode();
    final FragmentInstance instance = createFragmentInstance(objectPieceNode);

    Mockito.when(
            storageEngine.writeLoadTsFileObjectPieceNode(
                Mockito.eq(new DataRegionId(1)),
                Mockito.same(objectPieceNode),
                Mockito.eq("test-uuid")))
        .thenReturn(RpcUtils.SUCCESS_STATUS);

    dispatcher.dispatchLocally(instance);

    Mockito.verify(storageEngine)
        .writeLoadTsFileObjectPieceNode(
            Mockito.eq(new DataRegionId(1)),
            Mockito.same(objectPieceNode),
            Mockito.eq("test-uuid"));
  }

  @Test
  public void testDispatchLocallyObjectPieceNodeFailure() throws Exception {
    final StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    PowerMockito.mockStatic(StorageEngine.class);
    PowerMockito.when(StorageEngine.getInstance()).thenReturn(storageEngine);

    final LoadTsFileDispatcherImpl dispatcher = new LoadTsFileDispatcherImpl(null, false);
    dispatcher.setUuid("test-uuid");

    final LoadTsFileObjectPieceNode objectPieceNode = createObjectPieceNode();
    final FragmentInstance instance = createFragmentInstance(objectPieceNode);
    final TSStatus failureStatus = new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
    failureStatus.setMessage("object piece dispatch failed");

    Mockito.when(
            storageEngine.writeLoadTsFileObjectPieceNode(
                Mockito.eq(new DataRegionId(1)),
                Mockito.same(objectPieceNode),
                Mockito.eq("test-uuid")))
        .thenReturn(failureStatus);

    try {
      dispatcher.dispatchLocally(instance);
      Assert.fail("Expected FragmentInstanceDispatchException");
    } catch (final FragmentInstanceDispatchException e) {
      Assert.assertEquals(
          TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), e.getFailureStatus().getCode());
      Assert.assertEquals("object piece dispatch failed", e.getFailureStatus().getMessage());
    }
  }

  private static LoadTsFileObjectPieceNode createObjectPieceNode() {
    return new LoadTsFileObjectPieceNode(
        new PlanNodeId("object-piece"),
        new LoadTsFileObjectFileBatch(
            Collections.emptyList(), new TTimePartitionSlot().setStartTime(1L)));
  }

  private static FragmentInstance createFragmentInstance(
      final LoadTsFileObjectPieceNode objectPieceNode) {
    final PlanFragmentId fragmentId = new PlanFragmentId("test", 0);
    final FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(fragmentId, objectPieceNode),
            fragmentId.genFragmentInstanceId(),
            null,
            null,
            0L,
            null,
            false,
            false);
    instance.setExecutorAndHost(new StorageExecutor(createReplicaSet()));
    return instance;
  }

  private static TRegionReplicaSet createReplicaSet() {
    final TEndPoint endPoint = new TEndPoint().setIp("127.0.0.1").setPort(9000);
    final TDataNodeLocation dataNodeLocation =
        new TDataNodeLocation().setInternalEndPoint(endPoint);
    return new TRegionReplicaSet()
        .setRegionId(new DataRegionId(1).convertToTConsensusGroupId())
        .setDataNodeLocations(Collections.singletonList(dataNodeLocation));
  }
}
