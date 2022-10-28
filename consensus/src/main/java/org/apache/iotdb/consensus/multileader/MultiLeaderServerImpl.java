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

package org.apache.iotdb.consensus.multileader;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.MultiLeaderConfig;
import org.apache.iotdb.consensus.exception.ConsensusGroupAddPeerException;
import org.apache.iotdb.consensus.multileader.client.AsyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.client.SyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.multileader.snapshot.SnapshotFragmentReader;
import org.apache.iotdb.consensus.multileader.thrift.TActivatePeerReq;
import org.apache.iotdb.consensus.multileader.thrift.TActivatePeerRes;
import org.apache.iotdb.consensus.multileader.thrift.TBuildSyncLogChannelReq;
import org.apache.iotdb.consensus.multileader.thrift.TBuildSyncLogChannelRes;
import org.apache.iotdb.consensus.multileader.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.multileader.thrift.TCleanupTransferredSnapshotRes;
import org.apache.iotdb.consensus.multileader.thrift.TInactivatePeerReq;
import org.apache.iotdb.consensus.multileader.thrift.TInactivatePeerRes;
import org.apache.iotdb.consensus.multileader.thrift.TRemoveSyncLogChannelReq;
import org.apache.iotdb.consensus.multileader.thrift.TRemoveSyncLogChannelRes;
import org.apache.iotdb.consensus.multileader.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.multileader.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.multileader.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.multileader.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.consensus.multileader.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.multileader.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MultiLeaderServerImpl {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final String CONFIGURATION_TMP_FILE_NAME = "configuration.dat.tmp";
  private static final String SNAPSHOT_DIR_NAME = "snapshot";

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderServerImpl.class);

  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final Lock stateMachineLock = new ReentrantLock();
  private final Condition stateMachineCondition = stateMachineLock.newCondition();
  private final String storageDir;
  private final List<Peer> configuration;
  private final AtomicLong index;
  private final LogDispatcher logDispatcher;
  private final MultiLeaderConfig config;
  private final ConsensusReqReader reader;
  private boolean active;
  private String latestSnapshotId;
  private final IClientManager<TEndPoint, SyncMultiLeaderServiceClient> syncClientManager;
  private final MultiLeaderServerMetrics metrics;

  private final String consensusGroupId;

  public MultiLeaderServerImpl(
      String storageDir,
      Peer thisNode,
      List<Peer> configuration,
      IStateMachine stateMachine,
      IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager,
      IClientManager<TEndPoint, SyncMultiLeaderServiceClient> syncClientManager,
      MultiLeaderConfig config) {
    this.active = true;
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.syncClientManager = syncClientManager;
    this.configuration = configuration;
    if (configuration.isEmpty()) {
      recoverConfiguration();
    } else {
      persistConfiguration();
    }
    this.config = config;
    this.logDispatcher = new LogDispatcher(this, clientManager);
    reader = (ConsensusReqReader) stateMachine.read(new GetConsensusReqReaderPlan());
    long currentSearchIndex = reader.getCurrentSearchIndex();
    if (1 == configuration.size()) {
      // only one configuration means single replica.
      reader.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    }
    this.index = new AtomicLong(currentSearchIndex);
    this.consensusGroupId = thisNode.getGroupId().toString();
    this.metrics = new MultiLeaderServerMetrics(this);
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  public void start() {
    MetricService.getInstance().addMetricSet(this.metrics);
    stateMachine.start();
    logDispatcher.start();
  }

  public void stop() {
    logDispatcher.stop();
    stateMachine.stop();
    MetricService.getInstance().removeMetricSet(this.metrics);
  }

  /**
   * records the index of the log and writes locally, and then asynchronous replication is performed
   */
  public TSStatus write(IConsensusRequest request) {
    long consensusWriteStartTime = System.currentTimeMillis();
    stateMachineLock.lock();
    try {
      long getStateMachineLockTime = System.currentTimeMillis();
      // statistic the time of acquiring stateMachine lock
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              "getStateMachineLock",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(getStateMachineLockTime - consensusWriteStartTime);
      if (needBlockWrite()) {
        logger.info(
            "[Throttle Down] index:{}, safeIndex:{}",
            getIndex(),
            getCurrentSafelyDeletedSearchIndex());
        try {
          boolean timeout =
              !stateMachineCondition.await(
                  config.getReplication().getThrottleTimeOutMs(), TimeUnit.MILLISECONDS);
          if (timeout) {
            return RpcUtils.getStatus(
                TSStatusCode.WRITE_PROCESS_REJECT,
                "Reject write because there are too many requests need to process");
          }
        } catch (InterruptedException e) {
          logger.error("Failed to throttle down because ", e);
          Thread.currentThread().interrupt();
        }
      }
      long writeToStateMachineStartTime = System.currentTimeMillis();
      IndexedConsensusRequest indexedConsensusRequest =
          buildIndexedConsensusRequestForLocalRequest(request);
      // statistic the time of checking write block
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              "checkingBeforeWrite",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(writeToStateMachineStartTime - getStateMachineLockTime);
      if (indexedConsensusRequest.getSearchIndex() % 1000 == 0) {
        logger.info(
            "DataRegion[{}]: index after build: safeIndex:{}, searchIndex: {}",
            thisNode.getGroupId(),
            getCurrentSafelyDeletedSearchIndex(),
            indexedConsensusRequest.getSearchIndex());
      }
      // TODO wal and memtable
      TSStatus result = stateMachine.write(indexedConsensusRequest);
      long writeToStateMachineEndTime = System.currentTimeMillis();
      // statistic the time of writing request into stateMachine
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              "writeStateMachine",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(writeToStateMachineEndTime - writeToStateMachineStartTime);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // The index is used when constructing batch in LogDispatcher. If its value
        // increases but the corresponding request does not exist or is not put into
        // the queue, the dispatcher will try to find the request in WAL. This behavior
        // is not expected and will slow down the preparation speed for batch.
        // So we need to use the lock to ensure the `offer()` and `incrementAndGet()` are
        // in one transaction.
        synchronized (index) {
          logDispatcher.offer(indexedConsensusRequest);
          index.incrementAndGet();
        }
        // statistic the time of offering request into queue
        MetricService.getInstance()
            .getOrCreateHistogram(
                Metric.STAGE.toString(),
                MetricLevel.CORE,
                Tag.TYPE.toString(),
                "offerRequestToQueue",
                Tag.REGION.toString(),
                this.consensusGroupId)
            .update(System.currentTimeMillis() - writeToStateMachineEndTime);
      } else {
        logger.debug(
            "{}: write operation failed. searchIndex: {}. Code: {}",
            thisNode.getGroupId(),
            indexedConsensusRequest.getSearchIndex(),
            result.getCode());
      }
      // statistic the time of total write process
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.CORE,
              Tag.TYPE.toString(),
              "consensusWrite",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(System.currentTimeMillis() - consensusWriteStartTime);
      return result;
    } finally {
      stateMachineLock.unlock();
    }
  }

  public DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  public void takeSnapshot() throws ConsensusGroupAddPeerException {
    try {
      latestSnapshotId =
          String.format(
              "%s_%s_%d",
              SNAPSHOT_DIR_NAME, thisNode.getGroupId().getId(), System.currentTimeMillis());
      File snapshotDir = new File(storageDir, latestSnapshotId);
      if (snapshotDir.exists()) {
        FileUtils.deleteDirectory(snapshotDir);
      }
      if (!snapshotDir.mkdirs()) {
        throw new ConsensusGroupAddPeerException(
            String.format("%s: cannot mkdir for snapshot", thisNode.getGroupId()));
      }
      if (!stateMachine.takeSnapshot(snapshotDir)) {
        throw new ConsensusGroupAddPeerException("unknown error when taking snapshot");
      }
    } catch (IOException e) {
      throw new ConsensusGroupAddPeerException("error when taking snapshot", e);
    }
  }

  public void transitSnapshot(Peer targetPeer) throws ConsensusGroupAddPeerException {
    File snapshotDir = new File(storageDir, latestSnapshotId);
    List<Path> snapshotPaths = stateMachine.getSnapshotFiles(snapshotDir);
    logger.info("transit snapshots: {}", snapshotPaths);
    try (SyncMultiLeaderServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      for (Path path : snapshotPaths) {
        SnapshotFragmentReader reader = new SnapshotFragmentReader(latestSnapshotId, path);
        try {
          while (reader.hasNext()) {
            TSendSnapshotFragmentReq req = reader.next().toTSendSnapshotFragmentReq();
            req.setConsensusGroupId(targetPeer.getGroupId().convertToTConsensusGroupId());
            TSendSnapshotFragmentRes res = client.sendSnapshotFragment(req);
            if (!isSuccess(res.getStatus())) {
              throw new ConsensusGroupAddPeerException(
                  String.format("error when sending snapshot fragment to %s", targetPeer));
            }
          }
        } finally {
          reader.close();
        }
      }
    } catch (IOException | TException e) {
      throw new ConsensusGroupAddPeerException(
          String.format("error when send snapshot file to %s", targetPeer), e);
    }
  }

  public void receiveSnapshotFragment(
      String snapshotId, String originalFilePath, ByteBuffer fileChunk)
      throws ConsensusGroupAddPeerException {
    try {
      String targetFilePath = calculateSnapshotPath(snapshotId, originalFilePath);
      File targetFile = new File(storageDir, targetFilePath);
      Path parentDir = Paths.get(targetFile.getParent());
      if (!Files.exists(parentDir)) {
        Files.createDirectories(parentDir);
      }
      Files.write(
          Paths.get(targetFile.getAbsolutePath()),
          fileChunk.array(),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new ConsensusGroupAddPeerException(
          String.format("error when receiving snapshot %s", snapshotId), e);
    }
  }

  private String calculateSnapshotPath(String snapshotId, String originalFilePath)
      throws ConsensusGroupAddPeerException {
    if (!originalFilePath.contains(snapshotId)) {
      throw new ConsensusGroupAddPeerException(
          String.format(
              "invalid snapshot file. snapshotId: %s, filePath: %s", snapshotId, originalFilePath));
    }
    return originalFilePath.substring(originalFilePath.indexOf(snapshotId));
  }

  public void loadSnapshot(String snapshotId) {
    // TODO: (xingtanzjr) throw exception if the snapshot load failed
    stateMachine.loadSnapshot(new File(storageDir, snapshotId));
  }

  public void inactivePeer(Peer peer) throws ConsensusGroupAddPeerException {
    try (SyncMultiLeaderServiceClient client = syncClientManager.borrowClient(peer.getEndpoint())) {
      TInactivatePeerRes res =
          client.inactivatePeer(
              new TInactivatePeerReq(peer.getGroupId().convertToTConsensusGroupId()));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupAddPeerException(
            String.format("error when inactivating %s. %s", peer, res.getStatus()));
      }
    } catch (IOException | TException e) {
      throw new ConsensusGroupAddPeerException(
          String.format("error when inactivating %s", peer), e);
    }
  }

  public void triggerSnapshotLoad(Peer peer) throws ConsensusGroupAddPeerException {
    try (SyncMultiLeaderServiceClient client = syncClientManager.borrowClient(peer.getEndpoint())) {
      TTriggerSnapshotLoadRes res =
          client.triggerSnapshotLoad(
              new TTriggerSnapshotLoadReq(
                  thisNode.getGroupId().convertToTConsensusGroupId(), latestSnapshotId));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupAddPeerException(
            String.format("error when triggering snapshot load %s. %s", peer, res.getStatus()));
      }
    } catch (IOException | TException e) {
      throw new ConsensusGroupAddPeerException(String.format("error when activating %s", peer), e);
    }
  }

  public void activePeer(Peer peer) throws ConsensusGroupAddPeerException {
    try (SyncMultiLeaderServiceClient client = syncClientManager.borrowClient(peer.getEndpoint())) {
      TActivatePeerRes res =
          client.activatePeer(new TActivatePeerReq(peer.getGroupId().convertToTConsensusGroupId()));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupAddPeerException(
            String.format("error when activating %s. %s", peer, res.getStatus()));
      }
    } catch (IOException | TException e) {
      throw new ConsensusGroupAddPeerException(String.format("error when activating %s", peer), e);
    }
  }

  public void notifyPeersToBuildSyncLogChannel(Peer targetPeer)
      throws ConsensusGroupAddPeerException {
    // The configuration will be modified during iterating because we will add the targetPeer to
    // configuration
    List<Peer> currentMembers = new ArrayList<>(this.configuration);
    logger.info(
        "[MultiLeaderConsensus] notify current peers to build sync log. group member: {}, target: {}",
        currentMembers,
        targetPeer);
    for (Peer peer : currentMembers) {
      logger.info("[MultiLeaderConsensus] build sync log channel from {}", peer);
      if (peer.equals(thisNode)) {
        // use searchIndex for thisNode as the initialSyncIndex because targetPeer will load the
        // snapshot produced by thisNode
        buildSyncLogChannel(targetPeer, index.get());
      } else {
        // use RPC to tell other peers to build sync log channel to target peer
        try (SyncMultiLeaderServiceClient client =
            syncClientManager.borrowClient(peer.getEndpoint())) {
          TBuildSyncLogChannelRes res =
              client.buildSyncLogChannel(
                  new TBuildSyncLogChannelReq(
                      targetPeer.getGroupId().convertToTConsensusGroupId(),
                      targetPeer.getEndpoint(),
                      targetPeer.getNodeId()));
          if (!isSuccess(res.status)) {
            throw new ConsensusGroupAddPeerException(
                String.format("build sync log channel failed from %s to %s", peer, targetPeer));
          }
        } catch (IOException | TException e) {
          // We use a simple way to deal with the connection issue when notifying other nodes to
          // build sync log. If the un-responsible peer is the peer which will be removed, we cannot
          // suspend the operation and need to skip it. In order to keep the mechanism works fine,
          // we will skip the peer which cannot be reached.
          // If following error message appears, the un-responsible peer should be removed manually
          // after current operation
          // TODO: (xingtanzjr) design more reliable way for MultiLeaderConsensus
          logger.error(
              "cannot notify {} to build sync log channel. Please check the status of this node manually",
              peer,
              e);
        }
      }
    }
  }

  public void notifyPeersToRemoveSyncLogChannel(Peer targetPeer)
      throws ConsensusGroupAddPeerException {
    // The configuration will be modified during iterating because we will add the targetPeer to
    // configuration
    List<Peer> currentMembers = new ArrayList<>(this.configuration);
    for (Peer peer : currentMembers) {
      if (peer.equals(targetPeer)) {
        // if the targetPeer is the same as current peer, skip it because removing itself is illegal
        continue;
      }
      if (peer.equals(thisNode)) {
        removeSyncLogChannel(targetPeer);
      } else {
        // use RPC to tell other peers to build sync log channel to target peer
        try (SyncMultiLeaderServiceClient client =
            syncClientManager.borrowClient(peer.getEndpoint())) {
          TRemoveSyncLogChannelRes res =
              client.removeSyncLogChannel(
                  new TRemoveSyncLogChannelReq(
                      targetPeer.getGroupId().convertToTConsensusGroupId(),
                      targetPeer.getEndpoint(),
                      targetPeer.getNodeId()));
          if (!isSuccess(res.status)) {
            throw new ConsensusGroupAddPeerException(
                String.format("remove sync log channel failed from %s to %s", peer, targetPeer));
          }
        } catch (IOException | TException e) {
          throw new ConsensusGroupAddPeerException(
              String.format("error when removing sync log channel to %s", peer), e);
        }
      }
    }
  }

  private boolean isSuccess(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  /** build SyncLog channel with safeIndex as the default initial sync index */
  public void buildSyncLogChannel(Peer targetPeer) throws ConsensusGroupAddPeerException {
    buildSyncLogChannel(targetPeer, getCurrentSafelyDeletedSearchIndex());
  }

  public void buildSyncLogChannel(Peer targetPeer, long initialSyncIndex)
      throws ConsensusGroupAddPeerException {
    // step 1, build sync channel in LogDispatcher
    logger.info(
        "[MultiLeaderConsensus] build sync log channel to {} with initialSyncIndex {}",
        targetPeer,
        initialSyncIndex);
    logDispatcher.addLogDispatcherThread(targetPeer, initialSyncIndex);
    // step 2, update configuration
    configuration.add(targetPeer);
    // step 3, persist configuration
    logger.info("[MultiLeaderConsensus] persist new configuration: {}", configuration);
    persistConfigurationUpdate();
  }

  public void removeSyncLogChannel(Peer targetPeer) throws ConsensusGroupAddPeerException {
    try {
      // step 1, remove sync channel in LogDispatcher
      logDispatcher.removeLogDispatcherThread(targetPeer);
      logger.info("[MultiLeaderConsensus] log dispatcher to {} removed and cleanup", targetPeer);
      // step 2, update configuration
      configuration.remove(targetPeer);
      // step 3, persist configuration
      persistConfigurationUpdate();
      logger.info("[MultiLeaderConsensus] configuration updated to {}", this.configuration);
    } catch (IOException e) {
      throw new ConsensusGroupAddPeerException("error when remove LogDispatcherThread", e);
    }
  }

  public void persistConfiguration() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      serializeConfigurationTo(outputStream);
      Files.write(
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath()),
          publicBAOS.getBuf());
    } catch (IOException e) {
      // TODO: (xingtanzjr) need to handle the IOException because the MultiLeaderConsensus won't
      // work expectedly
      //  if the exception occurs
      logger.error("Unexpected error occurs when persisting configuration", e);
    }
  }

  public void persistConfigurationUpdate() throws ConsensusGroupAddPeerException {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      serializeConfigurationTo(outputStream);
      Path tmpConfigurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_TMP_FILE_NAME).getAbsolutePath());
      Path configurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath());
      Files.write(tmpConfigurationPath, publicBAOS.getBuf());
      Files.delete(configurationPath);
      Files.move(tmpConfigurationPath, configurationPath);
    } catch (IOException e) {
      throw new ConsensusGroupAddPeerException(
          "Unexpected error occurs when update configuration", e);
    }
  }

  private void serializeConfigurationTo(DataOutputStream outputStream) throws IOException {
    outputStream.writeInt(configuration.size());
    for (Peer peer : configuration) {
      peer.serialize(outputStream);
    }
  }

  public void recoverConfiguration() {
    ByteBuffer buffer;
    try {
      Path tmpConfigurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_TMP_FILE_NAME).getAbsolutePath());
      Path configurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath());
      // If the tmpConfigurationPath exists, it means the `persistConfigurationUpdate` is
      // interrupted
      // unexpectedly, we need substitute configuration with tmpConfiguration file
      if (Files.exists(tmpConfigurationPath)) {
        if (Files.exists(configurationPath)) {
          Files.delete(configurationPath);
        }
        Files.move(tmpConfigurationPath, configurationPath);
      }
      buffer = ByteBuffer.wrap(Files.readAllBytes(configurationPath));
      int size = buffer.getInt();
      for (int i = 0; i < size; i++) {
        configuration.add(Peer.deserialize(buffer));
      }
      logger.info("Recover multiLeader, configuration: {}", configuration);
    } catch (IOException e) {
      logger.error("Unexpected error occurs when recovering configuration", e);
    }
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForLocalRequest(
      IConsensusRequest request) {
    return new IndexedConsensusRequest(index.get() + 1, Collections.singletonList(request));
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForRemoteRequest(
      long syncIndex, List<IConsensusRequest> requests) {
    return new IndexedConsensusRequest(
        ConsensusReqReader.DEFAULT_SEARCH_INDEX, syncIndex, requests);
  }

  /**
   * In the case of multiple copies, the minimum synchronization index is selected. In the case of
   * single copies, the current index is selected
   */
  public long getCurrentSafelyDeletedSearchIndex() {
    return logDispatcher.getMinSyncIndex().orElseGet(index::get);
  }

  public String getStorageDir() {
    return storageDir;
  }

  public Peer getThisNode() {
    return thisNode;
  }

  public List<Peer> getConfiguration() {
    return configuration;
  }

  public long getIndex() {
    return index.get();
  }

  public MultiLeaderConfig getConfig() {
    return config;
  }

  public boolean needBlockWrite() {
    return reader.getTotalSize() > config.getReplication().getWalThrottleThreshold();
  }

  public boolean unblockWrite() {
    return reader.getTotalSize() < config.getReplication().getWalThrottleThreshold();
  }

  public void signal() {
    stateMachineLock.lock();
    try {
      stateMachineCondition.signalAll();
    } finally {
      stateMachineLock.unlock();
    }
  }

  public AtomicLong getIndexObject() {
    return index;
  }

  public boolean isReadOnly() {
    return stateMachine.isReadOnly();
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    logger.info("set {} active status to {}", this.thisNode, active);
    this.active = active;
  }

  public void cleanupRemoteSnapshot(Peer targetPeer) throws ConsensusGroupAddPeerException {
    try (SyncMultiLeaderServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      TCleanupTransferredSnapshotReq req =
          new TCleanupTransferredSnapshotReq(
              targetPeer.getGroupId().convertToTConsensusGroupId(), latestSnapshotId);
      TCleanupTransferredSnapshotRes res = client.cleanupTransferredSnapshot(req);
      if (!isSuccess(res.getStatus())) {
        throw new ConsensusGroupAddPeerException(
            String.format(
                "cleanup remote snapshot failed of %s ,status is %s", targetPeer, res.getStatus()));
      }
    } catch (IOException | TException e) {
      throw new ConsensusGroupAddPeerException(
          String.format("cleanup remote snapshot failed of %s", targetPeer), e);
    }
  }

  public void cleanupTransferredSnapshot(String snapshotId) throws ConsensusGroupAddPeerException {
    File snapshotDir = new File(storageDir, snapshotId);
    if (snapshotDir.exists()) {
      try {
        FileUtils.deleteDirectory(snapshotDir);
      } catch (IOException e) {
        throw new ConsensusGroupAddPeerException(e);
      }
    }
  }
}
