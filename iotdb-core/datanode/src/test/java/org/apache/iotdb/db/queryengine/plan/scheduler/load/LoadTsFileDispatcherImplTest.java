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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
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
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(StorageEngine.class)
public class LoadTsFileDispatcherImplTest {

  @Test
  public void testLoggedOversizedFrameRequiresTwoSlices() {
    Assert.assertEquals(
        2, LoadTsFileDispatcherImpl.getSliceCount(120_438_706, 64 * 1024 * 1024 - 1024));
  }

  @Test
  public void testSplitTsFilePieceReqWithinThriftFrameSize() throws Exception {
    final int thriftMaxFrameSize = 4096;
    final int bodySizeLimit = thriftMaxFrameSize - 1024;
    final byte[] body = new byte[thriftMaxFrameSize * 3];
    for (int i = 0; i < body.length; i++) {
      body[i] = (byte) i;
    }

    final List<TTsFilePieceReq> requests =
        LoadTsFileDispatcherImpl.splitTsFilePieceReq(
            ByteBuffer.wrap(body),
            "test-uuid",
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            bodySizeLimit);

    Assert.assertEquals(4, requests.size());
    final ByteBuffer assembledBody = ByteBuffer.allocate(body.length);
    for (int i = 0; i < requests.size(); i++) {
      final TTsFilePieceReq request = requests.get(i);
      Assert.assertEquals(i, request.getSliceIndex());
      Assert.assertEquals(requests.size(), request.getSliceCount());
      Assert.assertEquals(body.length, request.getOriginBodySize());
      Assert.assertTrue(request.body.remaining() <= bodySizeLimit);
      assembledBody.put(request.body.duplicate());

      final TMemoryBuffer memoryBuffer = new TMemoryBuffer(thriftMaxFrameSize);
      final TElasticFramedTransport transport =
          new TElasticFramedTransport(memoryBuffer, 128, thriftMaxFrameSize, true);
      try {
        new IDataNodeRPCService.Client(new TBinaryProtocol(transport))
            .send_sendTsFilePieceNode(request);
        final int frameSize = ByteBuffer.wrap(memoryBuffer.getArray()).getInt();
        Assert.assertEquals(memoryBuffer.length() - Integer.BYTES, frameSize);
        Assert.assertTrue(frameSize < thriftMaxFrameSize);
      } finally {
        transport.close();
      }
    }
    Assert.assertArrayEquals(body, assembledBody.array());
  }

  @Test
  public void testSmallTsFilePieceReqIsNotSliced() {
    final List<TTsFilePieceReq> requests =
        LoadTsFileDispatcherImpl.splitTsFilePieceReq(
            ByteBuffer.wrap(new byte[100]),
            "test-uuid",
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            1024);

    Assert.assertEquals(1, requests.size());
    Assert.assertFalse(requests.get(0).isSetSliceIndex());
    Assert.assertFalse(requests.get(0).isSetSliceCount());
    Assert.assertFalse(requests.get(0).isSetOriginBodySize());
  }

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
