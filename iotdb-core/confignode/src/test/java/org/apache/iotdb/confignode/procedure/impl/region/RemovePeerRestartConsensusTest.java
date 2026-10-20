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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher.LogDispatcherThread;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

/**
 * Runs the ConfigNode removal state machine against real IoTConsensus replication services. Only
 * ConfigNode persistence transport and the DataNode internal-service facade are adapted: metadata
 * changes use PartitionInfo, and the production reset RPC crosses a loopback Thrift connection. The
 * in-memory state machine stands in for DataRegion/WAL recovery; no full DataNode JVM is started.
 */
public class RemovePeerRestartConsensusTest {

  private static final DataRegionId GROUP = new DataRegionId(92);
  private static final TConsensusGroupId REGION = GROUP.convertToTConsensusGroupId();
  private static final int INITIAL_WRITES = 8;
  private static final int WRITES_DURING_STALE_MEMBERSHIP = 8;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void removalProcedureRepairsRestartedPeerAndWritesContinue() throws Exception {
    final ConfigNodeConfig cnConfig = ConfigNodeDescriptor.getInstance().getConf();
    final String originalProtocol = cnConfig.getDataRegionConsensusProtocolClass();
    cnConfig.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    final ConcurrentMap<Integer, IoTConsensus> servers = new ConcurrentHashMap<>();
    final List<LoopbackInternalService> internalServices = new ArrayList<>();
    final List<Peer> peers = new ArrayList<>();
    final List<TDataNodeLocation> locations = new ArrayList<>();
    final List<File> storageDirectories = new ArrayList<>();
    final List<MemoryStateMachine> stateMachines = new ArrayList<>();
    final ConcurrentMap<Integer, TResetPeerListReq> resetsReceived = new ConcurrentHashMap<>();
    try {
      for (int nodeId = 1; nodeId <= 3; nodeId++) {
        final LoopbackInternalService internal =
            new LoopbackInternalService(nodeId, servers, resetsReceived);
        internalServices.add(internal);
        final TEndPoint consensusEndpoint = new TEndPoint("127.0.0.1", unusedPort());
        locations.add(location(nodeId, internal.getEndpoint(), consensusEndpoint));
        peers.add(new Peer(GROUP, nodeId, consensusEndpoint));
        storageDirectories.add(temporaryFolder.newFolder("peer-" + nodeId));
        stateMachines.add(new MemoryStateMachine());
      }
      for (int index = 0; index < peers.size(); index++) {
        final IoTConsensus server =
            newServer(peers.get(index), storageDirectories.get(index), stateMachines.get(index));
        servers.put(peers.get(index).getNodeId(), server);
        server.start();
        server.createLocalPeer(GROUP, peers);
      }
      final Peer writer = peers.get(0);
      final Peer removed = peers.get(1);
      final Peer coordinator = peers.get(2);
      final List<Peer> finalPeers = Arrays.asList(writer, coordinator);
      final IoTConsensus originalWriter = servers.get(writer.getNodeId());
      write(originalWriter, INITIAL_WRITES);
      await("initial replication", () -> originalWriter.getImpl(GROUP).getSyncLag() == 0);
      final LogDispatcherThread originalRemovedThread =
          dispatcher(originalWriter.getImpl(GROUP), removed);

      final PartitionInfo partitionInfo = newPartitionInfo(locations);
      final ConfigManager configManager =
          Mockito.mock(ConfigManager.class, Mockito.RETURNS_DEEP_STUBS);
      Mockito.when(configManager.getPartitionManager().getAllReplicaSets())
          .thenAnswer(ignored -> partitionInfo.getAllReplicaSets());
      Mockito.when(
              configManager
                  .getPartitionManager()
                  .removeRegionLocation(any(RemoveRegionLocationPlan.class)))
          .thenAnswer(invocation -> partitionInfo.removeRegionLocation(invocation.getArgument(0)));
      final RegionMaintainHandler handler = Mockito.spy(new RegionMaintainHandler(configManager));
      // These adapters replace asynchronous DN task submission, not the procedure's final reset.
      Mockito.doNothing().when(handler).transferRegionLeader(any(), any(), any());
      Mockito.doAnswer(
              invocation -> {
                servers.get(coordinator.getNodeId()).removeRemotePeer(GROUP, removed);
                return success();
              })
          .when(handler)
          .submitRemoveRegionPeerTask(anyLong(), any(), any(), any());
      Mockito.doAnswer(
              invocation -> {
                servers.get(removed.getNodeId()).deleteLocalPeer(GROUP);
                return success();
              })
          .when(handler)
          .submitDeleteOldRegionPeerTask(anyLong(), any(), any());
      Mockito.doReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS))
          .when(handler)
          .waitTaskFinish(anyLong(), any(TDataNodeLocation.class));
      final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
      Mockito.when(env.getRegionMaintainHandler()).thenReturn(handler);
      final RemoveRegionPeerProcedure procedure =
          new RemoveRegionPeerProcedure(REGION, locations.get(2), locations.get(1));
      procedure.executeFromState(env, RemoveRegionPeerState.TRANSFER_REGION_LEADER);
      procedure.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER);
      assertEquals(finalPeers, originalWriter.getImpl(GROUP).getConfiguration());
      assertEquals(
          Collections.singletonList(coordinator), dispatcherPeers(originalWriter.getImpl(GROUP)));
      assertTrue(originalRemovedThread.isStopped());

      // Obtain the same persisted replica-set source used for DataNode restart responses while
      // the procedure is still between REMOVE_REGION_PEER and REMOVE_REGION_LOCATION_CACHE.
      final List<TDataNodeLocation> staleRestartLocations =
          partitionInfo.getAllReplicaSets(writer.getNodeId()).get(0).getDataNodeLocations();
      assertTrue(staleRestartLocations.contains(locations.get(1)));
      assertTrue(resetsReceived.isEmpty());
      procedure.executeFromState(env, RemoveRegionPeerState.DELETE_OLD_REGION_PEER);
      procedure.executeFromState(env, RemoveRegionPeerState.DROP_CONSENSUS_PIPES);
      originalWriter.stop();
      servers.remove(writer.getNodeId());
      final IoTConsensus restartedWriter =
          newServer(writer, storageDirectories.get(0), stateMachines.get(0));
      servers.put(writer.getNodeId(), restartedWriter);
      restartedWriter.recordCorrectPeerListBeforeStarting(
          Collections.singletonMap(GROUP, toPeers(staleRestartLocations)));
      restartedWriter.start();
      final IoTConsensusServerImpl recovered = restartedWriter.getImpl(GROUP);
      assertEquals(peers, recovered.getConfiguration());
      final LogDispatcherThread recreatedRemovedThread = dispatcher(recovered, removed);
      assertNotSame(originalRemovedThread, recreatedRemovedThread);
      assertFalse(recreatedRemovedThread.isStopped());
      write(restartedWriter, WRITES_DURING_STALE_MEMBERSHIP);
      final LogDispatcherThread healthyThread = dispatcher(recovered, coordinator);
      final long expectedIndex = INITIAL_WRITES + WRITES_DURING_STALE_MEMBERSHIP;
      await(
          "healthy peer receives new writes",
          () -> healthyThread.getCurrentSyncIndex() == expectedIndex);
      await(
          "removed peer fills its five-batch pipeline",
          () -> {
            synchronized (recreatedRemovedThread.getSyncStatus()) {
              return recreatedRemovedThread.getSyncStatus().getPendingBatches().size() == 5;
            }
          });
      assertEquals(INITIAL_WRITES, recreatedRemovedThread.getCurrentSyncIndex());
      assertEquals(WRITES_DURING_STALE_MEMBERSHIP, recovered.getSyncLag());
      printState("STALE_RESTART_BEFORE_PROCEDURE_FINALIZATION", recovered);

      // Only this production state transition may trigger reset. The test body never invokes it.
      procedure.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE);
      assertTrue(resetsReceived.isEmpty());
      procedure.executeFromState(env, RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST);
      assertEquals(
          new HashSet<>(Arrays.asList(writer.getNodeId(), coordinator.getNodeId())),
          resetsReceived.keySet());
      for (TResetPeerListReq request : resetsReceived.values()) {
        assertEquals(REGION, request.getRegionId());
        assertEquals(finalPeers, toPeers(request.getCorrectLocations()));
      }
      assertEquals(finalPeers, recovered.getConfiguration());
      assertEquals(Collections.singletonList(coordinator), dispatcherPeers(recovered));
      assertTrue(recreatedRemovedThread.isStopped());
      assertEquals(0, recovered.getSyncLag());
      assertFalse(
          partitionInfo
              .getAllReplicaSets(writer.getNodeId())
              .get(0)
              .getDataNodeLocations()
              .contains(locations.get(1)));
      // The earlier response remains stale; correctness comes from the production reset RPC.
      assertTrue(staleRestartLocations.contains(locations.get(1)));
      printState("PROCEDURE_FINALIZATION_REPAIRED_MEMBERSHIP", recovered);

      write(restartedWriter, 2);
      await("writes after procedure finalization", () -> recovered.getSyncLag() == 0);
      assertEquals(expectedIndex + 2, stateMachines.get(2).values.size());
      printState("WRITES_CONTINUE_AFTER_REPAIR", recovered);
      restartedWriter.stop();
      servers.remove(writer.getNodeId());
      final IoTConsensus freshWriter =
          newServer(writer, storageDirectories.get(0), stateMachines.get(0));
      servers.put(writer.getNodeId(), freshWriter);
      freshWriter.recordCorrectPeerListBeforeStarting(
          Collections.singletonMap(
              GROUP,
              toPeers(
                  partitionInfo
                      .getAllReplicaSets(writer.getNodeId())
                      .get(0)
                      .getDataNodeLocations())));
      freshWriter.start();
      assertEquals(finalPeers, freshWriter.getImpl(GROUP).getConfiguration());
      assertEquals(
          Collections.singletonList(coordinator), dispatcherPeers(freshWriter.getImpl(GROUP)));
      write(freshWriter, 1);
      await(
          "second restart uses committed membership",
          () -> freshWriter.getImpl(GROUP).getSyncLag() == 0);
      assertEquals(expectedIndex + 3, stateMachines.get(2).values.size());
      printState("SECOND_RESTART_DOES_NOT_RECREATE_REMOVED_PEER", freshWriter.getImpl(GROUP));
    } finally {
      servers.values().parallelStream().forEach(IoTConsensus::stop);
      for (LoopbackInternalService internal : internalServices) {
        internal.close();
      }
      cnConfig.setDataRegionConsensusProtocolClass(originalProtocol);
    }
  }

  private static PartitionInfo newPartitionInfo(List<TDataNodeLocation> locations) {
    final PartitionInfo partitionInfo = new PartitionInfo();
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.restart_repair")));
    final CreateRegionGroupsPlan groups = new CreateRegionGroupsPlan();
    groups.addRegionGroup(
        "root.restart_repair", new TRegionReplicaSet(REGION, new ArrayList<>(locations)));
    partitionInfo.createRegionGroups(groups);
    return partitionInfo;
  }

  private static TDataNodeLocation location(int nodeId, TEndPoint internal, TEndPoint consensus) {
    return new TDataNodeLocation(
        nodeId,
        new TEndPoint("127.0.0.1", 6667),
        internal,
        new TEndPoint("127.0.0.1", 10740),
        consensus,
        new TEndPoint("127.0.0.1", 10760));
  }

  private static List<Peer> toPeers(List<TDataNodeLocation> locations) {
    return locations.stream()
        .map(location -> Peer.valueOf(REGION, location))
        .sorted()
        .collect(Collectors.toList());
  }

  private static IoTConsensus newServer(
      Peer peer, File directory, MemoryStateMachine stateMachine) {
    return new IoTConsensus(
        ConsensusConfig.newBuilder()
            .setThisNodeId(peer.getNodeId())
            .setThisNode(peer.getEndpoint())
            .setStorageDir(directory.getAbsolutePath())
            .setConsensusGroupType(TConsensusGroupType.DataRegion)
            .setIoTConsensusConfig(
                IoTConsensusConfig.newBuilder()
                    .setRpc(
                        IoTConsensusConfig.RPC.newBuilder().setConnectionTimeoutInMs(500).build())
                    .setReplication(
                        IoTConsensusConfig.Replication.newBuilder()
                            .setCheckpointGap(1)
                            .setMaxLogEntriesNumPerBatch(1)
                            .setMaxPendingBatchesNum(5)
                            .setBasicRetryWaitTimeMs(100)
                            .setMaxRetryWaitTimeMs(500)
                            .build())
                    .build())
            .build(),
        ignored -> stateMachine);
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static int unusedPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void write(IoTConsensus server, int count) throws Exception {
    final int previousCount = (int) server.getImpl(GROUP).getSearchIndex();
    for (int index = 1; index <= count; index++) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          server.write(GROUP, new IntegerRequest(previousCount + index)).getCode());
    }
  }

  private static void await(String description, BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(description, condition.getAsBoolean());
  }

  @SuppressWarnings("unchecked")
  private static List<LogDispatcherThread> dispatchers(IoTConsensusServerImpl server)
      throws Exception {
    final LogDispatcher dispatcher = server.getLogDispatcher();
    final Field field = LogDispatcher.class.getDeclaredField("threads");
    field.setAccessible(true);
    synchronized (dispatcher) {
      return new ArrayList<>((List<LogDispatcherThread>) field.get(dispatcher));
    }
  }

  private static List<Peer> dispatcherPeers(IoTConsensusServerImpl server) throws Exception {
    return dispatchers(server).stream()
        .map(LogDispatcherThread::getPeer)
        .collect(Collectors.toList());
  }

  private static LogDispatcherThread dispatcher(IoTConsensusServerImpl server, Peer peer)
      throws Exception {
    return dispatchers(server).stream()
        .filter(thread -> thread.getPeer().equals(peer))
        .findFirst()
        .get();
  }

  private static void printState(String stage, IoTConsensusServerImpl server) throws Exception {
    System.out.printf(
        "CN_REPAIR %s searchIndex=%d minSyncIndex=%d syncLag=%d dispatchers=%s%n",
        stage,
        server.getSearchIndex(),
        server.getMinSyncIndex(),
        server.getSyncLag(),
        dispatchers(server).stream()
            .map(thread -> thread.getPeer().getNodeId() + ":" + thread.getCurrentSyncIndex())
            .collect(Collectors.toList()));
  }

  /** A real Thrift endpoint forwarding only resetPeerList to the selected consensus instance. */
  private static class LoopbackInternalService implements AutoCloseable {
    private final TEndPoint endpoint;
    private final ExecutorService workers;
    private final TThreadPoolServer service;
    private final Thread serverThread;

    private LoopbackInternalService(
        int nodeId,
        Map<Integer, IoTConsensus> servers,
        Map<Integer, TResetPeerListReq> resetsReceived)
        throws Exception {
      final ServerSocket socket = new ServerSocket(0);
      endpoint = new TEndPoint("127.0.0.1", socket.getLocalPort());
      final IDataNodeRPCService.Iface facade = Mockito.mock(IDataNodeRPCService.Iface.class);
      Mockito.when(facade.resetPeerList(any(TResetPeerListReq.class)))
          .thenAnswer(
              invocation -> {
                final TResetPeerListReq request = invocation.getArgument(0);
                servers.get(nodeId).resetPeerList(GROUP, toPeers(request.getCorrectLocations()));
                resetsReceived.put(nodeId, request.deepCopy());
                return success();
              });
      workers =
          Executors.newCachedThreadPool(
              runnable -> {
                final Thread thread = new Thread(runnable, "reset-peer-facade-" + nodeId);
                thread.setDaemon(true);
                return thread;
              });
      service =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(new TServerSocket(socket, 1000))
                  .processor(new IDataNodeRPCService.Processor<>(facade))
                  .protocolFactory(
                      CommonDescriptor.getInstance().getConfig().isRpcThriftCompressionEnabled()
                          ? new TCompactProtocol.Factory()
                          : new TBinaryProtocol.Factory())
                  .transportFactory(new TFramedTransport.Factory())
                  .executorService(workers)
                  .stopTimeoutVal(1)
                  .stopTimeoutUnit(TimeUnit.SECONDS));
      serverThread = new Thread(service::serve, "reset-peer-server-" + nodeId);
      serverThread.setDaemon(true);
      serverThread.start();
      await("internal Thrift facade starts", service::isServing);
    }

    private TEndPoint getEndpoint() {
      return endpoint;
    }

    @Override
    public void close() throws Exception {
      CnToDnInternalServiceAsyncRequestManager.getInstance().resetClient(endpoint);
      service.stop();
      workers.shutdownNow();
      serverThread.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  private static class IntegerRequest implements IConsensusRequest {
    private final int value;

    private IntegerRequest(int value) {
      this.value = value;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      return ByteBuffer.allocate(Integer.BYTES).putInt(value).flip();
    }
  }

  /** Keeps local WAL entries across consensus-instance restarts and records replicated values. */
  private static class MemoryStateMachine implements IStateMachine, ConsensusReqReader, DataSet {
    private final Map<Long, IndexedConsensusRequest> localRequests = new HashMap<>();
    private final Set<Integer> values = ConcurrentHashMap.newKeySet();
    private final AtomicLong localIndex = new AtomicLong();

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (request instanceof DeserializedBatchIndexedConsensusRequest) {
        final List<TSStatus> statuses = new ArrayList<>();
        for (IConsensusRequest element :
            ((DeserializedBatchIndexedConsensusRequest) request).getInsertNodes()) {
          write(element);
          statuses.add(success());
        }
        return success().setSubStatus(statuses);
      }
      final IndexedConsensusRequest indexed = (IndexedConsensusRequest) request;
      final List<IConsensusRequest> copied = new ArrayList<>();
      for (IConsensusRequest element : indexed.getRequests()) {
        final int value = element.serializeToByteBuffer().duplicate().getInt();
        values.add(value);
        copied.add(new IntegerRequest(value));
      }
      if (indexed.getSearchIndex() != DEFAULT_SEARCH_INDEX) {
        synchronized (localRequests) {
          localRequests.put(
              indexed.getSearchIndex(),
              new IndexedConsensusRequest(indexed.getSearchIndex(), copied));
          localIndex.set(indexed.getSearchIndex());
          localRequests.notifyAll();
        }
      }
      return success();
    }

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      if (!(request instanceof BatchIndexedConsensusRequest)) {
        return request;
      }
      final BatchIndexedConsensusRequest batch = (BatchIndexedConsensusRequest) request;
      final DeserializedBatchIndexedConsensusRequest result =
          new DeserializedBatchIndexedConsensusRequest(
              batch.getStartSyncIndex(),
              batch.getEndSyncIndex(),
              batch.getRequests().size(),
              batch.getSourcePeerId(),
              0);
      batch.getRequests().forEach(result::add);
      return result;
    }

    @Override
    public DataSet read(IConsensusRequest request) {
      return request instanceof GetConsensusReqReaderPlan ? this : null;
    }

    @Override
    public boolean takeSnapshot(File directory) {
      return true;
    }

    @Override
    public boolean loadSnapshot(File directory) {
      return true;
    }

    @Override
    public void setSafelyDeletedSearchIndex(long index) {}

    @Override
    public long getCurrentSearchIndex() {
      return localIndex.get();
    }

    @Override
    public long getCurrentWALFileVersion() {
      return 0;
    }

    @Override
    public long getTotalSize() {
      return 0;
    }

    @Override
    public ReqIterator getReqIterator(long startIndex) {
      return new ReqIterator() {
        private long nextIndex = startIndex;

        @Override
        public boolean hasNext() {
          synchronized (localRequests) {
            return localRequests.containsKey(nextIndex);
          }
        }

        @Override
        public IndexedConsensusRequest next() {
          synchronized (localRequests) {
            final IndexedConsensusRequest request = localRequests.get(nextIndex++);
            return new IndexedConsensusRequest(request.getSearchIndex(), request.getRequests());
          }
        }

        @Override
        public void waitForNextReady() throws InterruptedException {
          synchronized (localRequests) {
            while (!hasNext()) {
              localRequests.wait();
            }
          }
        }

        @Override
        public void waitForNextReady(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
          final long deadline = System.nanoTime() + unit.toNanos(timeout);
          synchronized (localRequests) {
            while (!hasNext()) {
              final long remaining = deadline - System.nanoTime();
              if (remaining <= 0) {
                throw new TimeoutException();
              }
              TimeUnit.NANOSECONDS.timedWait(localRequests, remaining);
            }
          }
        }

        @Override
        public void skipTo(long targetIndex) {
          nextIndex = targetIndex;
        }
      };
    }
  }
}
