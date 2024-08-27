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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
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
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.snapshot.IoTConsensusRateLimiter;
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
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.utils.FileUtils.humanReadableByteCountSI;

public class IoTConsensusServerImpl {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final String CONFIGURATION_TMP_FILE_NAME = "configuration.dat.tmp";
  public static final String SNAPSHOT_DIR_NAME = "snapshot";
  private static final Pattern SNAPSHOT_INDEX_PATTEN = Pattern.compile(".*[^\\d](?=(\\d+))");
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();
  private final Logger logger = LoggerFactory.getLogger(IoTConsensusServerImpl.class);
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final ConcurrentHashMap<Integer, SyncLogCacheQueue> cacheQueueMap;
  private final Lock stateMachineLock = new ReentrantLock();
  private final Condition stateMachineCondition = stateMachineLock.newCondition();
  private final String storageDir;
  private final List<Peer> configuration;
  private final AtomicLong searchIndex;
  private final LogDispatcher logDispatcher;
  private IoTConsensusConfig config;
  private final ConsensusReqReader consensusReqReader;
  private volatile boolean active;
  private String newSnapshotDirName;
  private final IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager;
  private final IoTConsensusServerMetrics ioTConsensusServerMetrics;
  private final String consensusGroupId;
  private final ScheduledExecutorService backgroundTaskService;
  private final IoTConsensusRateLimiter ioTConsensusRateLimiter =
      IoTConsensusRateLimiter.getInstance();
  private volatile long lastPinnedSearchIndexForMigration = -1;
  private volatile long lastPinnedSafeDeletedIndexForMigration = -1;

  public IoTConsensusServerImpl(
      String storageDir,
      Peer thisNode,
      List<Peer> configuration,
      IStateMachine stateMachine,
      ScheduledExecutorService backgroundTaskService,
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
    this.backgroundTaskService = backgroundTaskService;
    this.config = config;
    this.consensusGroupId = thisNode.getGroupId().toString();
    consensusReqReader = (ConsensusReqReader) stateMachine.read(new GetConsensusReqReaderPlan());
    this.searchIndex = new AtomicLong(consensusReqReader.getCurrentSearchIndex());
    this.ioTConsensusServerMetrics = new IoTConsensusServerMetrics(this);
    this.logDispatcher = new LogDispatcher(this, clientManager);
    // Since the underlying wal does not persist safelyDeletedSearchIndex, IoTConsensus needs to
    // update wal with its syncIndex recovered from the consensus layer when initializing.
    // This prevents wal from being piled up if the safelyDeletedSearchIndex is not updated after
    // the restart and Leader migration occurs
    checkAndUpdateSafeDeletedSearchIndex();
    // see message in logs for details
    checkAndUpdateSearchIndex();
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  public void start() {
    MetricService.getInstance().addMetricSet(this.ioTConsensusServerMetrics);
    stateMachine.start();
    logDispatcher.start();
  }

  public void stop() {
    logDispatcher.stop();
    stateMachine.stop();
    MetricService.getInstance().removeMetricSet(this.ioTConsensusServerMetrics);
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
      ioTConsensusServerMetrics.recordGetStateMachineLockTime(
          getStateMachineLockTime - consensusWriteStartTime);
      if (needBlockWrite()) {
        logger.info("[Throttle Down] index:{}, safeIndex:{}", getSearchIndex(), getMinSyncIndex());
        try {
          boolean timeout =
              !stateMachineCondition.await(
                  config.getReplication().getThrottleTimeOutMs(), TimeUnit.MILLISECONDS);
          if (timeout) {
            return RpcUtils.getStatus(
                TSStatusCode.WRITE_PROCESS_REJECT,
                String.format(
                    "The write is rejected because the wal directory size has reached the "
                        + "threshold %d bytes. You may need to adjust the flush policy of the "
                        + "storage storageengine or the IoTConsensus synchronization parameter",
                    config.getReplication().getWalThrottleThreshold()));
          }
        } catch (InterruptedException e) {
          logger.error("Failed to throttle down because ", e);
          Thread.currentThread().interrupt();
        }
      }
      long writeToStateMachineStartTime = System.nanoTime();
      // statistic the time of checking write block
      ioTConsensusServerMetrics.recordCheckingBeforeWriteTime(
          writeToStateMachineStartTime - getStateMachineLockTime);
      IndexedConsensusRequest indexedConsensusRequest =
          buildIndexedConsensusRequestForLocalRequest(request);
      if (indexedConsensusRequest.getSearchIndex() % 100000 == 0) {
        logger.info(
            "DataRegion[{}]: index after build: safeIndex:{}, searchIndex: {}",
            thisNode.getGroupId(),
            getMinSyncIndex(),
            indexedConsensusRequest.getSearchIndex());
      }
      IConsensusRequest planNode = stateMachine.deserializeRequest(indexedConsensusRequest);
      long startWriteTime = System.nanoTime();
      TSStatus result = stateMachine.write(planNode);
      PERFORMANCE_OVERVIEW_METRICS.recordEngineCost(System.nanoTime() - startWriteTime);

      long writeToStateMachineEndTime = System.nanoTime();
      // statistic the time of writing request into stateMachine
      ioTConsensusServerMetrics.recordWriteStateMachineTime(
          writeToStateMachineEndTime - writeToStateMachineStartTime);
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
        ioTConsensusServerMetrics.recordOfferRequestToQueueTime(
            System.nanoTime() - writeToStateMachineEndTime);
      } else {
        logger.debug(
            "{}: write operation failed. searchIndex: {}. Code: {}",
            thisNode.getGroupId(),
            indexedConsensusRequest.getSearchIndex(),
            result.getCode());
      }
      // statistic the time of total write process
      ioTConsensusServerMetrics.recordConsensusWriteTime(
          System.nanoTime() - consensusWriteStartTime);
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

  public void transmitSnapshot(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    File snapshotDir = new File(storageDir, newSnapshotDirName);
    List<File> snapshotPaths = stateMachine.getSnapshotFiles(snapshotDir);
    AtomicLong snapshotSizeSumAtomic = new AtomicLong();
    StringBuilder allFilesStr = new StringBuilder();
    snapshotPaths.forEach(
        file -> {
          long fileSize = file.length();
          snapshotSizeSumAtomic.addAndGet(fileSize);
          allFilesStr
              .append("\n")
              .append(file.getName())
              .append(" ")
              .append(humanReadableByteCountSI(fileSize));
        });
    final long snapshotSizeSum = snapshotSizeSumAtomic.get();
    long transitedSnapshotSizeSum = 0;
    long transitedFilesNum = 0;
    long startTime = System.nanoTime();
    logger.info(
        "[SNAPSHOT TRANSMISSION] Start to transmit snapshots ({} files, total size {}) from dir {}",
        snapshotPaths.size(),
        humanReadableByteCountSI(snapshotSizeSum),
        snapshotDir);
    logger.info(
        "[SNAPSHOT TRANSMISSION] All the files below shell be transmitted: {}", allFilesStr);
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      for (File file : snapshotPaths) {
        SnapshotFragmentReader reader =
            new SnapshotFragmentReader(newSnapshotDirName, file.toPath());
        try {
          while (reader.hasNext()) {
            // TODO: zero copy ?
            TSendSnapshotFragmentReq req = reader.next().toTSendSnapshotFragmentReq();
            req.setConsensusGroupId(targetPeer.getGroupId().convertToTConsensusGroupId());
            ioTConsensusRateLimiter.acquireTransitDataSizeWithRateLimiter(req.getChunkLength());
            TSendSnapshotFragmentRes res = client.sendSnapshotFragment(req);
            if (!isSuccess(res.getStatus())) {
              throw new ConsensusGroupModifyPeerException(
                  String.format(
                      "[SNAPSHOT TRANSMISSION] Error when transmitting snapshot fragment to %s",
                      targetPeer));
            }
          }
          transitedSnapshotSizeSum += reader.getTotalReadSize();
          transitedFilesNum++;
          logger.info(
              "[SNAPSHOT TRANSMISSION] The overall progress for dir {}: files {}/{} done, size {}/{} done, time {} passed. File {} done.",
              newSnapshotDirName,
              transitedFilesNum,
              snapshotPaths.size(),
              humanReadableByteCountSI(transitedSnapshotSizeSum),
              humanReadableByteCountSI(snapshotSizeSum),
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  (System.nanoTime() - startTime) / 1_000_000),
              file);
        } finally {
          reader.close();
        }
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format("[SNAPSHOT TRANSMISSION] Error when send snapshot file to %s", targetPeer),
          e);
    }
    logger.info(
        "[SNAPSHOT TRANSMISSION] After {}, successfully transmit all snapshots from dir {}",
        CommonDateTimeUtils.convertMillisecondToDurationStr(
            (System.nanoTime() - startTime) / 1_000_000),
        snapshotDir);
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
      try (FileOutputStream fos = new FileOutputStream(targetFile.getAbsolutePath(), true);
          FileChannel channel = fos.getChannel()) {
        channel.write(fileChunk.slice());
      }
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

  @FunctionalInterface
  public interface ThrowableFunction<T, R> {
    R apply(T t) throws Exception;
  }

  public void inactivePeer(Peer peer, boolean forDeletionPurpose)
      throws ConsensusGroupModifyPeerException {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(peer.getEndpoint())) {
      try {
        TInactivatePeerRes res =
            client.inactivatePeer(
                new TInactivatePeerReq(peer.getGroupId().convertToTConsensusGroupId())
                    .setForDeletionPurpose(forDeletionPurpose));
        if (!isSuccess(res.status)) {
          throw new ConsensusGroupModifyPeerException(
              String.format("error when inactivating %s. %s", peer, res.getStatus()));
        }
      } catch (Exception e) {
        throw new ConsensusGroupModifyPeerException(
            String.format("error when inactivating %s", peer), e);
      }
    } catch (ClientManagerException e) {
      throw new ConsensusGroupModifyPeerException(e);
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
        buildSyncLogChannel(targetPeer, lastPinnedSearchIndexForMigration);
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

  public void notifyPeersToRemoveSyncLogChannel(Peer targetPeer) {
    // The configuration will be modified during iterating because we will add the targetPeer to
    // configuration
    ImmutableList<Peer> currentMembers = ImmutableList.copyOf(this.configuration);
    removeSyncLogChannel(targetPeer);
    for (Peer peer : currentMembers) {
      if (peer.equals(targetPeer)) {
        // if the targetPeer is the same as current peer, skip it because removing itself is illegal
        continue;
      }
      if (!peer.equals(thisNode)) {
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
            logger.warn("removing sync log channel failed from {} to {}", peer, targetPeer);
          }
        } catch (Exception e) {
          logger.warn(
              "Exception happened during removing sync log channel from {} to {}",
              peer,
              targetPeer,
              e);
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
              "[WAIT LOG SYNC] {} SyncLog is completed. TargetIndex: {}, CurrentSyncIndex: {}",
              targetPeer,
              res.searchIndex,
              res.safeIndex);
          return;
        }
        logger.info(
            "[WAIT LOG SYNC] {} SyncLog is still in progress. TargetIndex: {}, CurrentSyncIndex: {}",
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

  public boolean hasReleaseAllRegionRelatedResource(ConsensusGroupId groupId) {
    return stateMachine.hasReleaseAllRegionRelatedResource(groupId);
  }

  public void waitReleaseAllRegionRelatedResource(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    long checkIntervalInMs = 10_000L;
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      while (true) {
        TWaitReleaseAllRegionRelatedResourceRes res =
            client.waitReleaseAllRegionRelatedResource(
                new TWaitReleaseAllRegionRelatedResourceReq(
                    targetPeer.getGroupId().convertToTConsensusGroupId()));
        if (res.releaseAllResource) {
          logger.info("[WAIT RELEASE] {} has released all region related resource", targetPeer);
          return;
        }
        logger.info("[WAIT RELEASE] {} is still releasing all region related resource", targetPeer);
        Thread.sleep(checkIntervalInMs);
      }
    } catch (ClientManagerException | TException e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(
              "error when waiting %s to release all region related resource. %s",
              targetPeer, e.getMessage()),
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format(
              "thread interrupted when waiting %s to release all region related resource. %s",
              targetPeer, e.getMessage()),
          e);
    }
  }

  private boolean isSuccess(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  /**
   * build SyncLog channel with safeIndex as the default initial sync index.
   *
   * @throws ConsensusGroupModifyPeerException
   */
  public void buildSyncLogChannel(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    buildSyncLogChannel(targetPeer, getMinSyncIndex());
  }

  public void buildSyncLogChannel(Peer targetPeer, long initialSyncIndex)
      throws ConsensusGroupModifyPeerException {
    KillPoint.setKillPoint(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE);
    // step 1, build sync channel in LogDispatcher
    logger.info(
        "[IoTConsensus] build sync log channel to {} with initialSyncIndex {}",
        targetPeer,
        initialSyncIndex);
    logDispatcher.addLogDispatcherThread(targetPeer, initialSyncIndex);
    // step 2, update configuration
    configuration.add(targetPeer);
    // step 3, persist configuration
    persistConfiguration();
    logger.info("[IoTConsensus] persist new configuration: {}", configuration);
  }

  /**
   * @return totally succeed
   */
  public boolean removeSyncLogChannel(Peer targetPeer) {
    // step 1, remove sync channel in LogDispatcher
    boolean exceptionHappened = false;
    String suggestion = "";
    try {
      logDispatcher.removeLogDispatcherThread(targetPeer);
    } catch (Exception e) {
      logger.warn(
          "[IoTConsensus] Exception happened during removing log dispatcher thread, but configuration.dat will still be removed.",
          e);
      suggestion = "It's suggested restart the DataNode to remove log dispatcher thread.";
      exceptionHappened = true;
    }
    if (!exceptionHappened) {
      logger.info(
          "[IoTConsensus] Log dispatcher thread to {} has been removed and cleanup", targetPeer);
    }
    // step 2, update configuration
    configuration.remove(targetPeer);
    checkAndUpdateSafeDeletedSearchIndex();
    // step 3, persist configuration
    persistConfiguration();
    logger.info("[IoTConsensus] Configuration updated to {}. {}", this.configuration, suggestion);
    return !exceptionHappened;
  }

  public void persistConfiguration() {
    try {
      removeDuplicateConfiguration();
      renameTmpConfigurationFileToRemoveSuffix();
      serializeConfigurationAndFsyncToDisk();
      deleteConfiguration();
      renameTmpConfigurationFileToRemoveSuffix();
    } catch (IOException e) {
      // TODO: (xingtanzjr) need to handle the IOException because the IoTConsensus won't
      // work expectedly
      //  if the exception occurs
      logger.error("Unexpected error occurs when persisting configuration", e);
    }
  }

  public void recoverConfiguration() {
    try {
      Path tmpConfigurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_TMP_FILE_NAME).getAbsolutePath());
      Path configurationPath =
          Paths.get(new File(storageDir, CONFIGURATION_FILE_NAME).getAbsolutePath());
      // If the tmpConfigurationPath exists, it means the `persistConfigurationUpdate` is
      // interrupted
      // unexpectedly, we need substitute configuration with tmpConfiguration file
      if (Files.exists(tmpConfigurationPath)) {
        Files.deleteIfExists(configurationPath);
        Files.move(tmpConfigurationPath, configurationPath);
      }
      if (Files.exists(configurationPath)) {
        recoverFromOldConfigurationFile(configurationPath);
      } else {
        // recover from split configuration file
        Path dirPath = Paths.get(storageDir);
        List<Peer> tmpPeerList = getConfiguration(dirPath, CONFIGURATION_TMP_FILE_NAME);
        configuration.addAll(tmpPeerList);
        List<Peer> peerList = getConfiguration(dirPath, CONFIGURATION_FILE_NAME);
        for (Peer peer : peerList) {
          if (!configuration.contains(peer)) {
            configuration.add(peer);
          }
        }
        persistConfiguration();
      }
      logger.info("Recover IoTConsensus server Impl, configuration: {}", configuration);
    } catch (IOException e) {
      logger.error("Unexpected error occurs when recovering configuration", e);
    }
  }

  // @Compatibility
  private void recoverFromOldConfigurationFile(Path oldConfigurationPath) throws IOException {
    // recover from old configuration file
    ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(oldConfigurationPath));
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      configuration.add(Peer.deserialize(buffer));
    }
    persistConfiguration();
  }

  public static String generateConfigurationDatFileName(int nodeId, String suffix) {
    return nodeId + "_" + suffix;
  }

  private List<Peer> getConfiguration(Path dirPath, String configurationFileName)
      throws IOException {
    ByteBuffer buffer;
    List<Peer> tmpConfiguration = new ArrayList<>();
    Path[] files =
        Files.walk(dirPath)
            .filter(Files::isRegularFile)
            .filter(filePath -> filePath.getFileName().toString().contains(configurationFileName))
            .toArray(Path[]::new);
    for (Path file : files) {
      buffer = ByteBuffer.wrap(Files.readAllBytes(file));
      tmpConfiguration.add(Peer.deserialize(buffer));
    }
    return tmpConfiguration;
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForLocalRequest(
      IConsensusRequest request) {
    if (request instanceof ComparableConsensusRequest) {
      final IoTProgressIndex iotProgressIndex =
          new IoTProgressIndex(thisNode.getNodeId(), searchIndex.get() + 1);
      ((ComparableConsensusRequest) request).setProgressIndex(iotProgressIndex);
    }
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
  public long getMinSyncIndex() {
    return logDispatcher.getMinSyncIndex().orElseGet(searchIndex::get);
  }

  public long getMinFlushedSyncIndex() {
    return lastPinnedSafeDeletedIndexForMigration == -1
        ? logDispatcher.getMinFlushedSyncIndex().orElseGet(searchIndex::get)
        : lastPinnedSafeDeletedIndexForMigration;
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
    long minSyncIndex = getMinSyncIndex();
    return getSearchIndex() - minSyncIndex;
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
    return consensusReqReader.getTotalSize() > config.getReplication().getWalThrottleThreshold();
  }

  public boolean unblockWrite() {
    return consensusReqReader.getTotalSize() < config.getReplication().getWalThrottleThreshold();
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

  public ScheduledExecutorService getBackgroundTaskService() {
    return backgroundTaskService;
  }

  public LogDispatcher getLogDispatcher() {
    return logDispatcher;
  }

  public IoTConsensusServerMetrics getIoTConsensusServerMetrics() {
    return this.ioTConsensusServerMetrics;
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

  public void cleanupSnapshot(String snapshotId) throws ConsensusGroupModifyPeerException {
    File snapshotDir = new File(storageDir, snapshotId);
    if (snapshotDir.exists()) {
      try {
        FileUtils.deleteDirectory(snapshotDir);
      } catch (IOException e) {
        throw new ConsensusGroupModifyPeerException(e);
      }
    } else {
      logger.info("File not exist: {}", snapshotDir);
    }
  }

  public void cleanupLocalSnapshot() {
    try {
      cleanupSnapshot(newSnapshotDirName);
      stateMachine.clearSnapshot();
    } catch (ConsensusGroupModifyPeerException e) {
      logger.warn(
          "Cleanup local snapshot fail. You may manually delete {}.", newSnapshotDirName, e);
    }
  }

  /**
   * We should set safelyDeletedSearchIndex to searchIndex before addPeer to avoid potential data
   * lost.
   */
  public void checkAndLockSafeDeletedSearchIndex() {
    lastPinnedSearchIndexForMigration = searchIndex.get();
    lastPinnedSafeDeletedIndexForMigration = getMinFlushedSyncIndex();
    consensusReqReader.setSafelyDeletedSearchIndex(getMinFlushedSyncIndex());
  }

  /**
   * We should unlock safelyDeletedSearchIndex after addPeer to avoid potential data accumulation.
   */
  public void checkAndUnlockSafeDeletedSearchIndex() {
    lastPinnedSearchIndexForMigration = -1;
    lastPinnedSafeDeletedIndexForMigration = -1;
    checkAndUpdateSafeDeletedSearchIndex();
  }

  /**
   * If there is only one replica, set it to Long.MAX_VALUE.、 If there are multiple replicas, get
   * the latest SafelyDeletedSearchIndex again. This enables wal to be deleted in a timely manner.
   */
  public void checkAndUpdateSafeDeletedSearchIndex() {
    if (configuration.size() == 1) {
      consensusReqReader.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    } else {
      consensusReqReader.setSafelyDeletedSearchIndex(getMinFlushedSyncIndex());
    }
  }

  public void checkAndUpdateSearchIndex() {
    long currentSearchIndex = searchIndex.get();
    long safelyDeletedSearchIndex = getMinFlushedSyncIndex();
    if (currentSearchIndex < safelyDeletedSearchIndex) {
      logger.warn(
          "The searchIndex for this region({}) is smaller than the safelyDeletedSearchIndex when "
              + "the node is restarted, which means that the data of the current region is not flushed "
              + "by the wal, but has been synchronized to other nodes. At this point, "
              + "different replicas have been inconsistent and cannot be automatically recovered. "
              + "To prevent subsequent logs from marking smaller searchIndex and exacerbating the "
              + "inconsistency, we manually set the searchIndex({}) to safelyDeletedSearchIndex({}) "
              + "here to reduce the impact of this problem in the future",
          consensusGroupId,
          currentSearchIndex,
          safelyDeletedSearchIndex);
      searchIndex.set(safelyDeletedSearchIndex);
    }
  }

  public TSStatus syncLog(int sourcePeerId, IConsensusRequest request) {
    return cacheQueueMap
        .computeIfAbsent(sourcePeerId, SyncLogCacheQueue::new)
        .cacheAndInsertLatestNode((DeserializedBatchIndexedConsensusRequest) request);
  }

  public String getConsensusGroupId() {
    return consensusGroupId;
  }

  private void serializeConfigurationAndFsyncToDisk() throws IOException {
    for (Peer peer : configuration) {
      String peerConfigurationFileName =
          generateConfigurationDatFileName(peer.getNodeId(), CONFIGURATION_TMP_FILE_NAME);
      FileOutputStream fileOutputStream =
          new FileOutputStream(new File(storageDir, peerConfigurationFileName));
      try (DataOutputStream outputStream = new DataOutputStream(fileOutputStream)) {
        peer.serialize(outputStream);
      } finally {
        try {
          fileOutputStream.flush();
          fileOutputStream.getFD().sync();
        } catch (IOException ignore) {
          // ignore sync exception
        }
      }
    }
  }

  private void renameTmpConfigurationFileToRemoveSuffix() throws IOException {
    try (Stream<Path> stream = Files.list(Paths.get(storageDir))) {
      List<Path> paths =
          stream
              .filter(Files::isRegularFile)
              .filter(
                  filePath ->
                      filePath.getFileName().toString().endsWith(CONFIGURATION_TMP_FILE_NAME))
              .collect(Collectors.toList());
      for (Path filePath : paths) {
        String targetPath =
            filePath.toString().replace(CONFIGURATION_TMP_FILE_NAME, CONFIGURATION_FILE_NAME);
        File targetFile = new File(targetPath);
        if (targetFile.exists()) {
          try {
            Files.delete(targetFile.toPath());
          } catch (IOException e) {
            logger.error("Unexpected error occurs when delete file: {}", targetPath, e);
          }
        }
        if (!filePath.toFile().renameTo(targetFile)) {
          logger.error("Unexpected error occurs when rename file: {} -> {}", filePath, targetPath);
        }
      }
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  private void deleteConfiguration() throws IOException {
    try (Stream<Path> stream = Files.list(Paths.get(storageDir))) {
      stream
          .filter(Files::isRegularFile)
          .filter(filePath -> filePath.getFileName().toString().endsWith(CONFIGURATION_FILE_NAME))
          .forEach(
              filePath -> {
                try {
                  Files.delete(filePath);
                } catch (IOException e) {
                  logger.error(
                      "Unexpected error occurs when deleting old configuration file {}",
                      filePath,
                      e);
                }
              });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  public void removeDuplicateConfiguration() {
    Set<Peer> seen = new HashSet<>();
    Iterator<Peer> it = configuration.iterator();

    while (it.hasNext()) {
      Peer peer = it.next();
      if (!seen.add(peer)) {
        it.remove();
      }
    }
  }

  /** This method is used for hot reload of IoTConsensusConfig. */
  public void reloadConsensusConfig(IoTConsensusConfig config) {
    this.config = config;
  }

  /**
   * This method is used for write of IoTConsensus SyncLog. By this method, we can keep write order
   * in follower the same as the leader. And besides order insurance, we can make the
   * deserialization of PlanNode to be concurrent
   */
  private class SyncLogCacheQueue {

    private final int sourcePeerId;
    private final Lock queueLock = new ReentrantLock();
    private final Condition queueSortCondition = queueLock.newCondition();
    private final PriorityQueue<DeserializedBatchIndexedConsensusRequest> requestCache;
    private long nextSyncIndex = -1;

    public SyncLogCacheQueue(int sourcePeerId) {
      this.sourcePeerId = sourcePeerId;
      this.requestCache = new PriorityQueue<>();
    }

    /**
     * This method is used for write of IoTConsensus SyncLog. By this method, we can keep write
     * order in follower the same as the leader. And besides order insurance, we can make the
     * deserialization of PlanNode to be concurrent
     */
    private TSStatus cacheAndInsertLatestNode(DeserializedBatchIndexedConsensusRequest request) {
      logger.debug(
          "cacheAndInsert start: source = {}, region = {}, queue size {}, startSyncIndex = {}, endSyncIndex = {}",
          sourcePeerId,
          consensusGroupId,
          requestCache.size(),
          request.getStartSyncIndex(),
          request.getEndSyncIndex());
      queueLock.lock();
      try {
        long insertStartTime = System.nanoTime();
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
            // although the timeout is triggered, current thread cannot write its request
            // if current thread does not hold the peek request. And there should be some
            // other thread who hold the peek request. In this scenario, current thread
            // should go into await again and wait until its request becoming peek request
            if (timeout
                && requestCache.peek() != null
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
          } catch (InterruptedException e) {
            logger.warn(
                "current waiting is interrupted. SyncIndex: {}. Exception: ",
                request.getStartSyncIndex(),
                e);
            Thread.currentThread().interrupt();
            break;
          }
        }
        long sortTime = System.nanoTime();
        ioTConsensusServerMetrics.recordSortCost(sortTime - insertStartTime);
        List<TSStatus> subStatus = new LinkedList<>();
        for (IConsensusRequest insertNode : request.getInsertNodes()) {
          insertNode.markAsGeneratedByRemoteConsensusLeader();
          subStatus.add(stateMachine.write(insertNode));
        }
        long applyTime = System.nanoTime();
        ioTConsensusServerMetrics.recordApplyCost(applyTime - sortTime);
        queueSortCondition.signalAll();
        logger.debug(
            "cacheAndInsert end: source = {}, region = {}, queue size {}, startSyncIndex = {}, endSyncIndex = {}, sortTime = {}ms, applyTime = {}ms",
            sourcePeerId,
            consensusGroupId,
            requestCache.size(),
            request.getStartSyncIndex(),
            request.getEndSyncIndex(),
            TimeUnit.NANOSECONDS.toMillis(sortTime - insertStartTime),
            TimeUnit.NANOSECONDS.toMillis(applyTime - sortTime));
        return new TSStatus().setSubStatus(subStatus);
      } finally {
        queueLock.unlock();
      }
    }
  }
}
