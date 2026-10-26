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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
  public void testDispatchUsesEachReceiversFrameLimit() throws Exception {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    final int originalMaxFrameSize = config.getThriftMaxFrameSize();
    final int localMaxFrameSize = 8192;
    config.setThriftMaxFrameSize(localMaxFrameSize);
    try {
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
          Mockito.mock(IClientManager.class);
      final byte[] body = new byte[12000];
      for (int i = 0; i < body.length; i++) {
        body[i] = (byte) i;
      }
      final LoadTsFilePieceNode pieceNode = Mockito.mock(LoadTsFilePieceNode.class);
      Mockito.when(pieceNode.serializeToByteBuffer()).thenReturn(ByteBuffer.wrap(body));
      final FragmentInstance instance = createFragmentInstance(pieceNode);
      final List<TDataNodeLocation> locations = new ArrayList<>();
      final List<List<TTsFilePieceReq>> requestsByReceiver = new ArrayList<>();
      final List<SyncDataNodeInternalServiceClient> clients = new ArrayList<>();
      for (final int receiverMaxFrameSize : new int[] {4096, 16384}) {
        final TEndPoint endPoint = new TEndPoint("127.0.0.1", receiverMaxFrameSize);
        locations.add(new TDataNodeLocation().setInternalEndPoint(endPoint));
        final SyncDataNodeInternalServiceClient client =
            Mockito.mock(SyncDataNodeInternalServiceClient.class);
        clients.add(client);
        Mockito.when(clientManager.borrowClient(endPoint)).thenReturn(client);
        Mockito.when(client.getThriftMaxFrameSize()).thenReturn(receiverMaxFrameSize);
        final List<TTsFilePieceReq> requests = new ArrayList<>();
        requestsByReceiver.add(requests);
        Mockito.when(client.sendTsFilePieceNode(Mockito.any()))
            .thenAnswer(
                invocation -> {
                  final TTsFilePieceReq request = invocation.getArgument(0);
                  requests.add(request);
                  assertFitsBothTransports(request, localMaxFrameSize, receiverMaxFrameSize);
                  return new TLoadResp(true);
                });
      }
      instance.getRegionReplicaSet().setDataNodeLocations(locations);
      try (LoadTsFileDispatcherImpl dispatcher =
          new LoadTsFileDispatcherImpl(clientManager, false)) {
        dispatcher.setUuid("test-uuid");
        for (int attempt = 0; attempt < 2; attempt++) {
          Assert.assertTrue(
              dispatcher
                  .dispatch(null, Collections.singletonList(instance))
                  .get(10, TimeUnit.SECONDS)
                  .isSuccessful());
        }
      }
      for (int receiver = 0; receiver < clients.size(); receiver++) {
        Mockito.verify(clients.get(receiver), Mockito.times(1)).getThriftMaxFrameSize();
        final List<TTsFilePieceReq> requests = requestsByReceiver.get(receiver);
        final int expectedSliceCount = receiver == 0 ? 4 : 2;
        Assert.assertEquals(expectedSliceCount * 2, requests.size());
        for (int attempt = 0; attempt < 2; attempt++) {
          final ByteBuffer assembled = ByteBuffer.allocate(body.length);
          for (int slice = 0; slice < expectedSliceCount; slice++) {
            final TTsFilePieceReq request = requests.get(attempt * expectedSliceCount + slice);
            Assert.assertEquals(slice, request.getSliceIndex());
            Assert.assertEquals(expectedSliceCount, request.getSliceCount());
            assembled.put(request.body.duplicate());
          }
          Assert.assertArrayEquals(body, assembled.array());
        }
      }
      Mockito.verify(pieceNode, Mockito.times(2)).serializeToByteBuffer();
    } finally {
      config.setThriftMaxFrameSize(originalMaxFrameSize);
    }
  }

  @Test
  public void testOlderReceiverStillAcceptsSmallPiece() throws Exception {
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
        Mockito.mock(IClientManager.class);
    final SyncDataNodeInternalServiceClient discoveryClient =
        Mockito.mock(SyncDataNodeInternalServiceClient.class);
    final SyncDataNodeInternalServiceClient transferClient =
        Mockito.mock(SyncDataNodeInternalServiceClient.class);
    Mockito.when(clientManager.borrowClient(Mockito.any()))
        .thenReturn(discoveryClient, transferClient);
    Mockito.when(discoveryClient.getThriftMaxFrameSize())
        .thenThrow(new TException(new TApplicationException(TApplicationException.UNKNOWN_METHOD)));
    Mockito.when(transferClient.sendTsFilePieceNode(Mockito.any())).thenReturn(new TLoadResp(true));
    final LoadTsFilePieceNode pieceNode =
        new LoadTsFilePieceNode(new PlanNodeId("piece"), new File("test.tsfile"));
    try (LoadTsFileDispatcherImpl dispatcher = new LoadTsFileDispatcherImpl(clientManager, false)) {
      dispatcher.setUuid("test-uuid");
      Assert.assertTrue(
          dispatcher
              .dispatch(null, Collections.singletonList(createFragmentInstance(pieceNode)))
              .get(10, TimeUnit.SECONDS)
              .isSuccessful());
    }
    final ArgumentCaptor<TTsFilePieceReq> request = ArgumentCaptor.forClass(TTsFilePieceReq.class);
    Mockito.verify(transferClient).sendTsFilePieceNode(request.capture());
    Assert.assertFalse(request.getValue().isSetSliceIndex());
    Assert.assertEquals(pieceNode.serializeToByteBuffer(), request.getValue().body);
    Mockito.verify(discoveryClient, Mockito.never()).sendTsFilePieceNode(Mockito.any());
  }

  @Test
  public void testFrameLimitDiscoveryFailureDoesNotSendPiece() throws Exception {
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
        Mockito.mock(IClientManager.class);
    final SyncDataNodeInternalServiceClient client =
        Mockito.mock(SyncDataNodeInternalServiceClient.class);
    Mockito.when(clientManager.borrowClient(Mockito.any())).thenReturn(client);
    Mockito.when(client.getThriftMaxFrameSize()).thenThrow(new TTransportException());
    final LoadTsFilePieceNode pieceNode =
        new LoadTsFilePieceNode(new PlanNodeId("piece"), new File("test.tsfile"));
    try (LoadTsFileDispatcherImpl dispatcher = new LoadTsFileDispatcherImpl(clientManager, false)) {
      dispatcher.setUuid("test-uuid");
      Assert.assertFalse(
          dispatcher
              .dispatch(null, Collections.singletonList(createFragmentInstance(pieceNode)))
              .get(10, TimeUnit.SECONDS)
              .isSuccessful());
    }
    Mockito.verify(client, Mockito.never()).sendTsFilePieceNode(Mockito.any());
  }

  private static void assertFitsBothTransports(
      final TTsFilePieceReq request, final int senderMaxFrameSize, final int receiverMaxFrameSize)
      throws Exception {
    final TMemoryBuffer wire = new TMemoryBuffer(senderMaxFrameSize);
    try (TElasticFramedTransport sender =
            new TElasticFramedTransport(wire, 128, senderMaxFrameSize, true);
        TElasticFramedTransport receiver =
            new TElasticFramedTransport(wire, 128, receiverMaxFrameSize, true)) {
      new IDataNodeRPCService.Client(new TBinaryProtocol(sender)).send_sendTsFilePieceNode(request);
      final TBinaryProtocol protocol = new TBinaryProtocol(receiver);
      protocol.readMessageBegin();
      final IDataNodeRPCService.sendTsFilePieceNode_args args =
          new IDataNodeRPCService.sendTsFilePieceNode_args();
      args.read(protocol);
      protocol.readMessageEnd();
      Assert.assertEquals(request, args.getReq());
    }
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
            0,
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
