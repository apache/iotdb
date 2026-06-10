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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.rpc.RpcUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.Collections;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(StorageEngine.class)
public class LoadTsFileDispatcherImplTest {

  @Test
  public void testDispatchLocallyPieceNodeSkipsSerdeRoundTrip() throws Exception {
    final StorageEngine storageEngine = Mockito.mock(StorageEngine.class);
    PowerMockito.mockStatic(StorageEngine.class);
    PowerMockito.when(StorageEngine.getInstance()).thenReturn(storageEngine);

    final LoadTsFileDispatcherImpl dispatcher = new LoadTsFileDispatcherImpl(null, false);
    dispatcher.setUuid("test-uuid");

    final LoadTsFilePieceNode pieceNode =
        new LoadTsFilePieceNode(new PlanNodeId("piece"), new File("test.tsfile"));
    final FragmentInstance instance = createFragmentInstance(pieceNode);

    Mockito.when(
            storageEngine.writeLoadTsFileNode(
                Mockito.eq(new DataRegionId(1)), Mockito.same(pieceNode), Mockito.eq("test-uuid")))
        .thenReturn(RpcUtils.SUCCESS_STATUS);

    dispatcher.dispatchLocally(instance);

    Mockito.verify(storageEngine)
        .writeLoadTsFileNode(
            Mockito.eq(new DataRegionId(1)), Mockito.same(pieceNode), Mockito.eq("test-uuid"));
  }

  private static FragmentInstance createFragmentInstance(final LoadTsFilePieceNode pieceNode) {
    final PlanFragmentId fragmentId = new PlanFragmentId("test", 0);
    final FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(fragmentId, pieceNode),
            fragmentId.genFragmentInstanceId(),
            null,
            null,
            0,
            null,
            false,
            false);
    final TConsensusGroupId consensusGroupId = new DataRegionId(1).convertToTConsensusGroupId();
    instance.setExecutorAndHost(
        new StorageExecutor(
            new TRegionReplicaSet(
                consensusGroupId,
                Collections.singletonList(
                    new TDataNodeLocation().setInternalEndPoint(new TEndPoint("127.0.0.1", 1))))));
    return instance;
  }
}
