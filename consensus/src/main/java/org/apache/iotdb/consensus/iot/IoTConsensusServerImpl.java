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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.SyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.snapshot.SnapshotFragmentReader;
import org.apache.iotdb.consensus.iot.thrift.TActivatePeerReq;
import org.apache.iotdb.consensus.iot.thrift.TActivatePeerRes;
import org.apache.iotdb.consensus.iot.thrift.TBuildSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TBuildSyncLogChannelRes;
import org.apache.iotdb.consensus.iot.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.iot.thrift.TCleanupTransferredSnapshotRes;
import org.apache.iotdb.consensus.iot.thrift.TInactivatePeerReq;
import org.apache.iotdb.consensus.iot.thrift.TInactivatePeerRes;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelRes;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteRes;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.wal.GetConsensusReqReaderPlan;
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
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class IoTConsensusServerImpl {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final String CONFIGURATION_TMP_FILE_NAME = "configuration.dat.tmp";
  public static final String SNAPSHOT_DIR_NAME = "snapshot";
  private static final Pattern SNAPSHOT_INDEX_PATTEN = Pattern.compile(".*[^\\d](?=(\\d+))");
  private final Logger logger = LoggerFactory.getLogger(IoTConsensusServerImpl.class);
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final ConcurrentHashMap<String, SyncLogCacheQueue> cacheQueueMap;
  private final Lock stateMachineLock = new ReentrantLock();
  private final Condition stateMachineCondition = stateMachineLock.newCondition();
  private final String storageDir;
  private final List<Peer> configuration;
  private final AtomicLong searchIndex;
  private final LogDispatcher logDispatcher;
  private final IoTConsensusConfig config;
  private final ConsensusReqReader reader;
  private volatile boolean active;
  private String newSnapshotDirName;
  private final IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager;
  private final IoTConsensusServerMetrics metrics;

  private final String consensusGroupId;

  public IoTConsensusServerImpl(
      String storageDir,
      Peer thisNode,
      List<Peer> configuration,
      IStateMachine stateMachine,
      IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager,
      IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager,
      IoTConsensusConfig config) {
    this.active = true;
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.cacheQueueMap = new ConcurrentHashMap<>();
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
    checkAndUpdateSafeDeletedSearchIndex();
    this.searchIndex = new AtomicLong(currentSearchIndex);
    this.consensusGroupId = thisNode.getGroupId().toString();
    this.metrics = new IoTConsensusServerMetrics(this);
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
   * records the index of the log and writes locally, and then asynchronous replication is
   * performed.
   */
  public TSStatus write(IConsensusRequest request) {
    long consensusWriteStartTime = System.nanoTime();
    stateMachineLock.lock();
    try {
      long getStateMachineLockTime = System.nanoTime();
      // statistic the time of acquiring stateMachine lock
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              Metric.IOT_CONSENSUS.toString(),
              Tag.TYPE.toString(),
              "getStateMachineLock",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(getStateMachineLockTime - consensusWriteStartTime);
      if (needBlockWrite()) {
        logger.info(
            "[Throttle Down] index:{}, safeIndex:{}",
            getSearchIndex(),
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
      long writeToStateMachineStartTime = System.nanoTime();
      // statistic the time of checking write block
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              Metric.IOT_CONSENSUS.toString(),
              Tag.TYPE.toString(),
              "checkingBeforeWrite",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(writeToStateMachineStartTime - getStateMachineLockTime);
      IndexedConsensusRequest indexedConsensusRequest =
          buildIndexedConsensusRequestForLocalRequest(request);
      if (indexedConsensusRequest.getSearchIndex() % 1000 == 0) {
        logger.info(
            "DataRegion[{}]: index after build: safeIndex:{}, searchIndex: {}",
            thisNode.getGroupId(),
            getCurrentSafelyDeletedSearchIndex(),
            indexedConsensusRequest.getSearchIndex());
      }
      IConsensusRequest planNode = stateMachine.deserializeRequest(indexedConsensusRequest);
      long startWriteTime = System.nanoTime();
      TSStatus result = stateMachine.write(planNode);
      MetricService.getInstance()
          .timer(
              System.nanoTime() - startWriteTime,
              TimeUnit.NANOSECONDS,
              Metric.PERFORMANCE_OVERVIEW_STORAGE_DETAIL.toString(),
              MetricLevel.IMPORTANT,
              Tag.STAGE.toString(),
              PerformanceOverviewMetrics.ENGINE);

      long writeToStateMachineEndTime = System.nanoTime();
      // statistic the time of writing request into stateMachine
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.STAGE.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              Metric.IOT_CONSENSUS.toString(),
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
        synchronized (searchIndex) {
          logDispatcher.offer(indexedConsensusRequest);
          searchIndex.incrementAndGet();
        }
        // statistic the time of offering request into queue
        MetricService.getInstance()
            .getOrCreateHistogram(
                Metric.STAGE.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                Metric.IOT_CONSENSUS.toString(),
                Tag.TYPE.toString(),
                "offerRequestToQueue",
                Tag.REGION.toString(),
                this.consensusGroupId)
            .update(System.nanoTime() - writeToStateMachineEndTime);
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
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              Metric.IOT_CONSENSUS.toString(),
              Tag.TYPE.toString(),
              "consensusWrite",
              Tag.REGION.toString(),
              this.consensusGroupId)
          .update(System.nanoTime() - consensusWriteStartTime);
      return result;
    } finally {
      stateMachineLock.unlock();
    }
  }

  public DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  public void takeSnapshot() throws ConsensusGroupModifyPeerException {
    try {
      long newSnapshotIndex = getLatestSnapshotIndex() + 1;
      newSnapshotDirName =
          String.format(
              "%s_%s_%d", SNAPSHOT_DIR_NAME, thisNode.getGroupId().getId(), newSnapshotIndex);
      File snapshotDir = new File(storageDir, newSnapshotDirName);
      if (snapshotDir.exists()) {
        FileUtils.deleteDirectory(snapshotDir);
      }
      if (!snapshotDir.mkdirs()) {
        throw new ConsensusGroupModifyPeerException(
            String.format("%s: cannot mkdir for snapshot", thisNode.getGroupId()));
      }
      if (!stateMachine.takeSnapshot(snapshotDir)) {
        throw new ConsensusGroupModifyPeerException("unknown error when taking snapshot");
      }
      clearOldSnapshot();
    } catch (IOException e) {
      throw new ConsensusGroupModifyPeerException("error when taking snapshot", e);
    }
  }

  public void transitSnapshot(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    File snapshotDir = new File(storageDir, newSnapshotDirName);
    List<Path> snapshotPaths = stateMachine.getSnapshotFiles(snapshotDir);
    logger.info("transit snapshots: {}", snapshotPaths);
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      for (Path path : snapshotPaths) {
        SnapshotFragmentReader reader = new SnapshotFragmentReader(newSnapshotDirName, path);
        try {
          while (reader.hasNext()) {
            TSendSnapshotFragmentReq req = reader.next().toTSendSnapshotFragmentReq();
            req.setConsensusGroupId(targetPeer.getGroupId().convertToTConsensusGroupId());
            TSendSnapshotFragmentRes res = client.sendSnapshotFragment(req);
            if (!isSuccess(res.getStatus())) {
              throw new ConsensusGroupModifyPeerException(
                  String.format("error when sending snapshot fragment to %s", targetPeer));
            }
          }
        } finally {
          reader.close();
        }
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format("error when send snapshot file to %s", targetPeer), e);
    }
  }

  public void receiveSnapshotFragment(
      String snapshotId, String originalFilePath, ByteBuffer fileChunk)
      throws ConsensusGroupModifyPeerException {
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
      throw new ConsensusGroupModifyPeerException(
          String.format("error when receiving snapshot %s", snapshotId), e);
    }
  }

  private String calculateSnapshotPath(String snapshotId, String originalFilePath)
      throws ConsensusGroupModifyPeerException {
    if (!originalFilePath.contains(snapshotId)) {
      throw new ConsensusGroupModifyPeerException(
          String.format(
              "invalid snapshot file. snapshotId: %s, filePath: %s", snapshotId, originalFilePath));
    }
    return originalFilePath.substring(originalFilePath.indexOf(snapshotId));
  }

  private long getLatestSnapshotIndex() {
    long snapShotIndex = 0;
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(SNAPSHOT_DIR_NAME));
    if (versionFiles == null || versionFiles.length == 0) {
      return snapShotIndex;
    }
    for (File file : versionFiles) {
      snapShotIndex =
          Math.max(
              snapShotIndex,
              Long.parseLong(SNAPSHOT_INDEX_PATTEN.matcher(file.getName()).replaceAll("")));
    }
    return snapShotIndex;
  }

  private void clearOldSnapshot() {
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(SNAPSHOT_DIR_NAME));
    if (versionFiles == null || versionFiles.length == 0) {
      logger.error(
          "Can not find any snapshot dir after build a new snapshot for group {}",
          thisNode.getGroupId());
      return;
    }
    for (File file : versionFiles) {
      if (!file.getName().equals(newSnapshotDirName)) {
        try {
          FileUtils.deleteDirectory(file);
        } catch (IOException e) {
          logger.error("Delete old snapshot dir {} failed", file.getAbsolutePath(), e);
        }
      }
    }
  }

  public void loadSnapshot(String snapshotId) {
    // TODO: (xingtanzjr) throw exception if the snapshot load failed
    stateMachine.loadSnapshot(new File(storageDir, snapshotId));
  }

  public void inactivePeer(Peer peer) throws ConsensusGroupModifyPeerException {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(peer.getEndpoint())) {
      TInactivatePeerRes res =
          client.inactivatePeer(
              new TInactivatePeerReq(peer.getGroupId().convertToTConsensusGroupId()));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupModifyPeerException(
            String.format("error when inactivating %s. %s", peer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format("error when inactivating %s", peer), e);
    }
  }

  public void triggerSnapshotLoad(Peer peer) throws ConsensusGroupModifyPeerException {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(peer.getEndpoint())) {
      TTriggerSnapshotLoadRes res =
          client.triggerSnapshotLoad(
              new TTriggerSnapshotLoadReq(
                  thisNode.getGroupId().convertToTConsensusGroupId(), newSnapshotDirName));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupModifyPeerException(
            String.format("error when triggering snapshot load %s. %s", peer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format("error when activating %s", peer), e);
    }
  }

  public void activePeer(Peer peer) throws ConsensusGroupModifyPeerException {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(peer.getEndpoint())) {
      TActivatePeerRes res =
          client.activatePeer(new TActivatePeerReq(peer.getGroupId().convertToTConsensusGroupId()));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupModifyPeerException(
            String.format("error when activating %s. %s", peer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format("error when activating %s", peer), e);
    }
  }

  public void notifyPeersToBuildSyncLogChannel(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    // The configuration will be modified during iterating because we will add the targetPeer to
    // configuration
    List<Peer> currentMembers = new ArrayList<>(this.configuration);
    logger.info(
        "[IoTConsensus] notify current peers to build sync log. group member: {}, target: {}",
        currentMembers,
        targetPeer);
    for (Peer peer : currentMembers) {
      logger.info("[IoTConsensus] build sync log channel from {}", peer);
      if (peer.equals(thisNode)) {
        // use searchIndex for thisNode as the initialSyncIndex because targetPeer will load the
        // snapshot produced by thisNode
        buildSyncLogChannel(targetPeer, searchIndex.get());
      } else {
        // use RPC to tell other peers to build sync log channel to target peer
        try (SyncIoTConsensusServiceClient client =
            syncClientManager.borrowClient(peer.getEndpoint())) {
          TBuildSyncLogChannelRes res =
              client.buildSyncLogChannel(
                  new TBuildSyncLogChannelReq(
                      targetPeer.getGroupId().convertToTConsensusGroupId(),
                      targetPeer.getEndpoint(),
                      targetPeer.getNodeId()));
          if (!isSuccess(res.status)) {
            throw new ConsensusGroupModifyPeerException(
                String.format("build sync log channel failed from %s to %s", peer, targetPeer));
          }
        } catch (Exception e) {
          // We use a simple way to deal with the connection issue when notifying other nodes to
          // build sync log. If the un-responsible peer is the peer which will be removed, we cannot
          // suspend the operation and need to skip it. In order to keep the mechanism works fine,
          // we will skip the peer which cannot be reached.
          // If following error message appears, the un-responsible peer should be removed manually
          // after current operation
          // TODO: (xingtanzjr) design more reliable way for IoTConsensus
          logger.error(
              "cannot notify {} to build sync log channel. "
                  + "Please check the status of this node manually",
              peer,
              e);
        }
      }
    }
  }

  public void notifyPeersToRemoveSyncLogChannel(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
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
        try (SyncIoTConsensusServiceClient client =
            syncClientManager.borrowClient(peer.getEndpoint())) {
          TRemoveSyncLogChannelRes res =
              client.removeSyncLogChannel(
                  new TRemoveSyncLogChannelReq(
                      targetPeer.getGroupId().convertToTConsensusGroupId(),
                      targetPeer.getEndpoint(),
                      targetPeer.getNodeId()));
          if (!isSuccess(res.status)) {
            throw new ConsensusGroupModifyPeerException(
                String.format("remove sync log channel failed from %s to %s", peer, targetPeer));
          }
        } catch (Exception e) {
          throw new ConsensusGroupModifyPeerException(
              String.format("error when removing sync log channel to %s", peer), e);
        }
      }
    }
  }

  public void waitTargetPeerUntilSyncLogCompleted(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    long checkIntervalInMs = 10_000L;
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      while (true) {
        TWaitSyncLogCompleteRes res =
            client.waitSyncLogComplete(
                new TWaitSyncLogCompleteReq(targetPeer.getGroupId().convertToTConsensusGroupId()));
        if (res.complete) {
          logger.info(
              "{} SyncLog is completed. TargetIndex: {}, CurrentSyncIndex: {}",
              targetPeer,
              res.searchIndex,
              res.safeIndex);
          return;
        }
        logger.info(
            "{} SyncLog is still in progress. TargetIndex: {}, CurrentSyncIndex: {}",
            targetPeer,
            res.searchIndex,
            res.safeIndex);
        Thread.sleep(checkIntervalInMs);
      }
    } catch (ClientManagerException | TException e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(
              "error when waiting %s to complete SyncLog. %s", targetPeer, e.getMessage()),
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format(
              "thread interrupted when waiting %s to complete SyncLog. %s",
              targetPeer, e.getMessage()),
          e);
    }
  }

  private boolean isSuccess(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  /** build SyncLog channel with safeIndex as the default initial sync index */
  public void buildSyncLogChannel(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    buildSyncLogChannel(targetPeer, getCurrentSafelyDeletedSearchIndex());
  }

  public void buildSyncLogChannel(Peer targetPeer, long initialSyncIndex)
      throws ConsensusGroupModifyPeerException {
    // step 1, build sync channel in LogDispatcher
    logger.info(
        "[IoTConsensus] build sync log channel to {} with initialSyncIndex {}",
        targetPeer,
        initialSyncIndex);
    logDispatcher.addLogDispatcherThread(targetPeer, initialSyncIndex);
    // step 2, update configuration
    configuration.add(targetPeer);
    // step 3, persist configuration
    logger.info("[IoTConsensus] persist new configuration: {}", configuration);
    persistConfigurationUpdate();
  }

  public void removeSyncLogChannel(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    try {
      // step 1, remove sync channel in LogDispatcher
      logDispatcher.removeLogDispatcherThread(targetPeer);
      logger.info("[IoTConsensus] log dispatcher to {} removed and cleanup", targetPeer);
      // step 2, update configuration
      configuration.remove(targetPeer);
      checkAndUpdateSafeDeletedSearchIndex();
      // step 3, persist configuration
      persistConfigurationUpdate();
      logger.info("[IoTConsensus] configuration updated to {}", this.configuration);
    } catch (IOException e) {
      throw new ConsensusGroupModifyPeerException("error when remove LogDispatcherThread", e);
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
      // TODO: (xingtanzjr) need to handle the IOException because the IoTConsensus won't
      // work expectedly
      //  if the exception occurs
      logger.error("Unexpected error occurs when persisting configuration", e);
    }
  }

  public void persistConfigurationUpdate() throws ConsensusGroupModifyPeerException {
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
      throw new ConsensusGroupModifyPeerException(
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
      logger.info("Recover IoTConsensus server Impl, configuration: {}", configuration);
    } catch (IOException e) {
      logger.error("Unexpected error occurs when recovering configuration", e);
    }
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForLocalRequest(
      IConsensusRequest request) {
    return new IndexedConsensusRequest(searchIndex.get() + 1, Collections.singletonList(request));
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
    return logDispatcher.getMinSyncIndex().orElseGet(searchIndex::get);
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

  public long getSearchIndex() {
    return searchIndex.get();
  }

  public long getSyncLag() {
    long safeIndex = getCurrentSafelyDeletedSearchIndex();
    return getSearchIndex() - safeIndex;
  }

  public IoTConsensusConfig getConfig() {
    return config;
  }

  public long getLogEntriesFromWAL() {
    return logDispatcher.getLogEntriesFromWAL();
  }

  public long getLogEntriesFromQueue() {
    return logDispatcher.getLogEntriesFromQueue();
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
    return searchIndex;
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

  public void cleanupRemoteSnapshot(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      TCleanupTransferredSnapshotReq req =
          new TCleanupTransferredSnapshotReq(
              targetPeer.getGroupId().convertToTConsensusGroupId(), newSnapshotDirName);
      TCleanupTransferredSnapshotRes res = client.cleanupTransferredSnapshot(req);
      if (!isSuccess(res.getStatus())) {
        throw new ConsensusGroupModifyPeerException(
            String.format(
                "cleanup remote snapshot failed of %s ,status is %s", targetPeer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format("cleanup remote snapshot failed of %s", targetPeer), e);
    }
  }

  public void cleanupTransferredSnapshot(String snapshotId)
      throws ConsensusGroupModifyPeerException {
    File snapshotDir = new File(storageDir, snapshotId);
    if (snapshotDir.exists()) {
      try {
        FileUtils.deleteDirectory(snapshotDir);
      } catch (IOException e) {
        throw new ConsensusGroupModifyPeerException(e);
      }
    }
  }

  /**
   * We should set safelyDeletedSearchIndex to searchIndex before addPeer to avoid potential data
   * lost.
   */
  public void checkAndLockSafeDeletedSearchIndex() {
    if (configuration.size() == 1) {
      reader.setSafelyDeletedSearchIndex(searchIndex.get());
    }
  }

  /**
   * only one configuration means single replica, then we can set safelyDeletedSearchIndex to
   * Long.MAX_VALUE.
   */
  public void checkAndUpdateSafeDeletedSearchIndex() {
    if (configuration.size() == 1) {
      reader.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    }
  }

  public TSStatus syncLog(String sourcePeerId, IConsensusRequest request) {
    return cacheQueueMap
        .computeIfAbsent(sourcePeerId, SyncLogCacheQueue::new)
        .cacheAndInsertLatestNode((DeserializedBatchIndexedConsensusRequest) request);
  }

  /**
   * This method is used for write of IoTConsensus SyncLog. By this method, we can keep write order
   * in follower the same as the leader. And besides order insurance, we can make the
   * deserialization of PlanNode to be concurrent
   */
  private class SyncLogCacheQueue {
    private final String sourcePeerId;
    private final Lock queueLock = new ReentrantLock();
    private final Condition queueSortCondition = queueLock.newCondition();
    private final PriorityQueue<DeserializedBatchIndexedConsensusRequest> requestCache;
    private long nextSyncIndex = -1;

    public SyncLogCacheQueue(String sourcePeerId) {
      this.sourcePeerId = sourcePeerId;
      this.requestCache = new PriorityQueue<>();
    }

    /**
     * This method is used for write of IoTConsensus SyncLog. By this method, we can keep write
     * order in follower the same as the leader. And besides order insurance, we can make the
     * deserialization of PlanNode to be concurrent
     */
    private TSStatus cacheAndInsertLatestNode(DeserializedBatchIndexedConsensusRequest request) {
      queueLock.lock();
      try {
        requestCache.add(request);
        // If the peek is not hold by current thread, it should notify the corresponding thread to
        // process the peek when the queue is full
        if (requestCache.size() == config.getReplication().getMaxPendingBatchesNum()
            && requestCache.peek() != null
            && requestCache.peek().getStartSyncIndex() != request.getStartSyncIndex()) {
          queueSortCondition.signalAll();
        }
        while (true) {
          // If current InsertNode is the next target InsertNode, write it
          if (request.getStartSyncIndex() == nextSyncIndex) {
            requestCache.remove(request);
            nextSyncIndex = request.getEndSyncIndex() + 1;
            break;
          }
          // If all write thread doesn't hit nextSyncIndex and the heap is full, write
          // the peek request. This is used to keep the whole write correct when nextSyncIndex
          // is not set. We won't persist the value of nextSyncIndex to reduce the complexity.
          // There are some cases that nextSyncIndex is not set:
          //   1. When the system was just started
          //   2. When some exception occurs during SyncLog
          if (requestCache.size() == config.getReplication().getMaxPendingBatchesNum()
              && requestCache.peek() != null
              && requestCache.peek().getStartSyncIndex() == request.getStartSyncIndex()) {
            requestCache.remove();
            nextSyncIndex = request.getEndSyncIndex() + 1;
            break;
          }
          try {
            boolean timeout =
                !queueSortCondition.await(
                    config.getReplication().getMaxWaitingTimeForWaitBatchInMs(),
                    TimeUnit.MILLISECONDS);
            if (timeout) {
              // although the timeout is triggered, current thread cannot write its request
              // if current thread does not hold the peek request. And there should be some
              // other thread who hold the peek request. In this scenario, current thread
              // should go into await again and wait until its request becoming peek request
              if (requestCache.peek() != null
                  && requestCache.peek().getStartSyncIndex() == request.getStartSyncIndex()) {
                // current thread hold the peek request thus it can write the peek immediately.
                logger.info(
                    "waiting target request timeout. current index: {}, target index: {}",
                    request.getStartSyncIndex(),
                    nextSyncIndex);
                requestCache.remove(request);
                nextSyncIndex = Math.max(nextSyncIndex, request.getEndSyncIndex() + 1);
                break;
              }
            }
          } catch (InterruptedException e) {
            logger.warn(
                "current waiting is interrupted. SyncIndex: {}. Exception: {}",
                request.getStartSyncIndex(),
                e);
            Thread.currentThread().interrupt();
          }
        }
        logger.debug(
            "source = {}, region = {}, queue size {}, startSyncIndex = {}, endSyncIndex = {}",
            sourcePeerId,
            consensusGroupId,
            requestCache.size(),
            request.getStartSyncIndex(),
            request.getEndSyncIndex());
        List<TSStatus> subStatus = new LinkedList<>();
        for (IConsensusRequest insertNode : request.getInsertNodes()) {
          subStatus.add(stateMachine.write(insertNode));
        }
        queueSortCondition.signalAll();
        return new TSStatus().setSubStatus(subStatus);
      } finally {
        queueLock.unlock();
      }
    }
  }
}
