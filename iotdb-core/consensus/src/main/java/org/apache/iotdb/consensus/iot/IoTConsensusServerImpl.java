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
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.i18n.ConsensusMessages;
import org.apache.iotdb.consensus.i18n.IoTConsensusMessages;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.SyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.snapshot.IoTConsensusRateLimiter;
import org.apache.iotdb.consensus.iot.snapshot.SnapshotFragmentReader;
import org.apache.iotdb.consensus.iot.subscription.SubscriptionQueueRegistry;
import org.apache.iotdb.consensus.iot.subscription.SubscriptionWalRetentionCalculator;
import org.apache.iotdb.consensus.iot.subscription.SubscriptionWalRetentionCalculator.SubscriptionRetentionBound;
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
import org.apache.iotdb.consensus.iot.thrift.TSyncWriterSafeTimeBarrierReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncWriterSafeTimeBarrierRes;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.utils.FileUtils.humanReadableByteCountSI;

public class IoTConsensusServerImpl {

  public static final String SNAPSHOT_DIR_NAME = "snapshot";
  private static final String WRITER_META_FILE_NAME = "writer.meta";
  // writer.meta only protects writer physical-time monotonicity after restart, so avoid fsync per
  // write.
  private static final long WRITER_META_PERSIST_MIN_INTERVAL_MS = 1_000L;
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
  private FolderManager recvFolderManager = null;

  /**
   * Per-snapshotId map of TsFile group key ({@code fileKey}) to the chosen receive folder. It keeps
   * all companion files of one TsFile ({@code .tsfile}/{@code .tsfile.resource}/{@code
   * .tsfile.mods2}/...) in the same receive folder, so the load phase can hard-link them inside a
   * single data dir instead of falling back to a cross-disk copy. The {@code fileKey} rule matches
   * {@code SnapshotLoader#createLinksFromSnapshotToSourceDir} so grouping is consistent end to end.
   * Entries are removed once the snapshot is loaded or cleaned up.
   */
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>>
      snapshotReceiveFolderMap = new ConcurrentHashMap<>();

  private final TreeSet<Peer> configuration;
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
  private IndexedConsensusRequest lastConsensusRequest;

  // Subscription queues receive IndexedConsensusRequest in real-time from write(),
  // similar to LogDispatcher, enabling in-memory data delivery without waiting for WAL flush.
  private final SubscriptionQueueRegistry subscriptionQueueRegistry;
  private final SubscriptionWalRetentionCalculator subscriptionWalRetentionCalculator;

  /** Current routing epoch for ordered consensus subscription. Set by external routing changes. */
  private volatile long currentRoutingEpoch = 0;

  /**
   * Maximum physical time known to this replica. Local writes assign from it; remote replication
   * can also raise it so future local writes do not regress behind observed remote events.
   */
  private final AtomicLong lastAssignedPhysicalTime = new AtomicLong(0);

  private final WriterSafeFrontierTracker writerSafeFrontierTracker =
      new WriterSafeFrontierTracker();

  private final Path writerMetaPath;
  private final Object writerMetaPersistLock = new Object();
  private volatile WriterMeta latestWriterMeta;
  private long lastPersistedWriterLocalSeq = -1;
  private long lastPersistedWriterPhysicalTime = -1;
  private long lastWriterMetaPersistAttemptTimeMs = 0;

  public IoTConsensusServerImpl(
      String storageDir,
      List<String> recvSnapshotDirs,
      DirectoryStrategyType recvFolderStrategyType,
      Peer thisNode,
      TreeSet<Peer> configuration,
      IStateMachine stateMachine,
      ScheduledExecutorService backgroundTaskService,
      IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager,
      IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager,
      IoTConsensusConfig config)
      throws DiskSpaceInsufficientException {
    this.active = true;
    this.storageDir = storageDir;
    List<String> snapshotDirs = new ArrayList<>();
    if (recvSnapshotDirs != null) {
      for (String dir : recvSnapshotDirs) {
        snapshotDirs.add(dir + File.separator + SNAPSHOT_DIR_NAME);
      }
    } else {
      snapshotDirs.add(storageDir);
    }

    this.recvFolderManager = new FolderManager(snapshotDirs, recvFolderStrategyType);
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.cacheQueueMap = new ConcurrentHashMap<>();
    this.syncClientManager = syncClientManager;
    this.configuration = configuration;
    this.backgroundTaskService = backgroundTaskService;
    this.config = config;
    this.consensusGroupId = thisNode.getGroupId().toString();
    this.consensusReqReader =
        (ConsensusReqReader) stateMachine.read(new GetConsensusReqReaderPlan());
    this.searchIndex = new AtomicLong(consensusReqReader.getCurrentSearchIndex());
    this.subscriptionQueueRegistry = new SubscriptionQueueRegistry(consensusGroupId);
    this.subscriptionWalRetentionCalculator =
        new SubscriptionWalRetentionCalculator(consensusReqReader);
    this.writerMetaPath = Paths.get(storageDir, WRITER_META_FILE_NAME);
    initializeWriterMeta();
    this.ioTConsensusServerMetrics = new IoTConsensusServerMetrics(this);
    this.logDispatcher = new LogDispatcher(this, clientManager);
  }

  public IStateMachine getStateMachine() {
    return stateMachine;
  }

  public void start() {
    checkAndUpdateIndex();
    MetricService.getInstance().addMetricSet(this.ioTConsensusServerMetrics);
    stateMachine.start();
    logDispatcher.start();
  }

  public void stop() {
    logDispatcher.stop();
    persistLatestWriterMeta(true);
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
        logger.info(IoTConsensusMessages.THROTTLE_DOWN, getSearchIndex(), getMinSyncIndex());
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
          logger.error(IoTConsensusMessages.FAILED_TO_THROTTLE_DOWN, e);
          Thread.currentThread().interrupt();
        }
      }
      long writeToStateMachineStartTime = System.nanoTime();
      // statistic the time of checking write block
      ioTConsensusServerMetrics.recordCheckingBeforeWriteTime(
          writeToStateMachineStartTime - getStateMachineLockTime);
      IndexedConsensusRequest indexedConsensusRequest =
          buildIndexedConsensusRequestForLocalRequest(request);
      indexedConsensusRequest.setRoutingEpoch(currentRoutingEpoch);
      lastConsensusRequest = indexedConsensusRequest;
      if (indexedConsensusRequest.getSearchIndex() % 100000 == 0) {
        logger.info(
            IoTConsensusMessages.DATA_REGION_INDEX_AFTER_BUILD,
            thisNode.getGroupId(),
            getMinSyncIndex(),
            indexedConsensusRequest.getSearchIndex(),
            lastConsensusRequest.getSerializedRequests());
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
        writerSafeFrontierTracker.recordAppliedProgress(
            indexedConsensusRequest.getPhysicalTime(),
            thisNode.getNodeId(),
            indexedConsensusRequest.getLocalSeq());
        // The index is used when constructing batch in LogDispatcher. If its value
        // increases but the corresponding request does not exist or is not put into
        // the queue, the dispatcher will try to find the request in WAL. This behavior
        // is not expected and will slow down the preparation speed for batch.
        // So we need to use the lock to ensure the `offer()` and `incrementAndGet()` are
        // in one transaction.
        synchronized (searchIndex) {
          logDispatcher.offer(indexedConsensusRequest);
          // Deliver to subscription queues for real-time in-memory consumption.
          // Offer AFTER stateMachine.write() so that InsertNode has inferred types
          // and properly typed values (same timing as LogDispatcher).
          final int sqCount = subscriptionQueueRegistry.size();
          if (sqCount > 0) {
            subscriptionQueueRegistry.offer(indexedConsensusRequest);
          } else if (logger.isDebugEnabled()
              && indexedConsensusRequest.getSearchIndex() % 50 == 0) {
            // Log periodically when no subscription queues are registered
            logger.debug(
                IoTConsensusMessages.LOG_WRITE_NO_SUBSCRIPTION_QUEUES_REGISTERED_0F4E697B
                    + IoTConsensusMessages.LOG_GROUP_ARG_SEARCHINDEX_ARG_ARG_D5E034E6,
                consensusGroupId,
                indexedConsensusRequest.getSearchIndex(),
                System.identityHashCode(this));
          }
          searchIndex.incrementAndGet();
        }
        latestWriterMeta =
            new WriterMeta(
                indexedConsensusRequest.getLocalSeq(), indexedConsensusRequest.getPhysicalTime());
        // statistic the time of offering request into queue
        ioTConsensusServerMetrics.recordOfferRequestToQueueTime(
            System.nanoTime() - writeToStateMachineEndTime);
      } else if (logger.isDebugEnabled()) {
        logger.debug(
            IoTConsensusMessages
                .LOG_ARG_WRITE_OPERATION_FAILED_SEARCHINDEX_ARG_CODE_ARG_SUBSCRIPTIONQUEUES_ARG_THIS_ARG_F4B17576,
            thisNode.getGroupId(),
            indexedConsensusRequest.getSearchIndex(),
            result.getCode(),
            subscriptionQueueRegistry.size(),
            System.identityHashCode(this));
      }
      // statistic the time of total write process
      ioTConsensusServerMetrics.recordConsensusWriteTime(
          System.nanoTime() - consensusWriteStartTime);
      return result;
    } finally {
      stateMachineLock.unlock();
      persistLatestWriterMeta(false);
    }
  }

  public DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  public void takeSnapshot() throws ConsensusGroupModifyPeerException {
    try {
      newSnapshotDirName =
          String.format(
              "%s_%s_%s", SNAPSHOT_DIR_NAME, thisNode.getGroupId().getId(), UUID.randomUUID());
      File snapshotDir = new File(storageDir, newSnapshotDirName);
      if (snapshotDir.exists()) {
        FileUtils.deleteDirectory(snapshotDir);
      }
      if (!snapshotDir.mkdirs()) {
        throw new ConsensusGroupModifyPeerException(
            String.format(IoTConsensusMessages.CANNOT_MKDIR_FOR_SNAPSHOT, thisNode.getGroupId()));
      }
      if (!stateMachine.takeSnapshot(snapshotDir)) {
        throw new ConsensusGroupModifyPeerException(
            IoTConsensusMessages.UNKNOWN_ERROR_TAKING_SNAPSHOT);
      }
      clearOldSnapshot();
    } catch (IOException e) {
      throw new ConsensusGroupModifyPeerException(IoTConsensusMessages.ERROR_TAKING_SNAPSHOT, e);
    }
  }

  public void transmitSnapshot(Peer targetPeer) throws ConsensusGroupModifyPeerException {
    File snapshotDir = new File(storageDir, newSnapshotDirName);
    List<File> snapshotPaths = stateMachine.getSnapshotFiles(snapshotDir);
    long snapshotSizeSum = 0;
    for (File file : snapshotPaths) {
      snapshotSizeSum += file.length();
    }
    long transitedSnapshotSizeSum = 0;
    long transitedFilesNum = 0;
    long startTime = System.nanoTime();
    long lastProgressLogTime = startTime;
    // Throttle the per-file progress log to at most once per this interval; a snapshot may contain
    // hundreds of thousands of files, so one INFO line per file is itself a heavy cost.
    long progressLogIntervalNs =
        TimeUnit.MILLISECONDS.toNanos(
            config.getReplication().getSnapshotTransmissionProgressLogIntervalMs());
    logger.info(
        IoTConsensusMessages.SNAPSHOT_TRANSMISSION_START,
        snapshotPaths.size(),
        humanReadableByteCountSI(snapshotSizeSum),
        snapshotDir);
    if (logger.isDebugEnabled()) {
      StringBuilder allFilesStr = new StringBuilder();
      for (File file : snapshotPaths) {
        allFilesStr
            .append("\n")
            .append(file.getName())
            .append(" ")
            .append(humanReadableByteCountSI(file.length()));
      }
      logger.debug(IoTConsensusMessages.SNAPSHOT_TRANSMISSION_ALL_FILES, allFilesStr);
    }
    ByteBuffer fragmentBuffer =
        ByteBuffer.allocate(SnapshotFragmentReader.DEFAULT_FILE_FRAGMENT_SIZE);
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      for (File file : snapshotPaths) {
        SnapshotFragmentReader reader =
            new SnapshotFragmentReader(newSnapshotDirName, file.toPath(), fragmentBuffer);
        try {
          while (reader.hasNext()) {
            // TODO: zero copy ?
            TSendSnapshotFragmentReq req = reader.next().toTSendSnapshotFragmentReq();
            req.setConsensusGroupId(targetPeer.getGroupId().convertToTConsensusGroupId());
            ioTConsensusRateLimiter.acquireTransitDataSizeWithRateLimiter(req.getChunkLength());
            TSendSnapshotFragmentRes res = client.sendSnapshotFragment(req);
            if (!isSuccess(res.getStatus())) {
              throw new ConsensusGroupModifyPeerException(
                  String.format(IoTConsensusMessages.SNAPSHOT_TRANSMISSION_ERROR, targetPeer));
            }
          }
          transitedSnapshotSizeSum += reader.getTotalReadSize();
          transitedFilesNum++;
          long now = System.nanoTime();
          if (now - lastProgressLogTime >= progressLogIntervalNs
              || transitedFilesNum == snapshotPaths.size()) {
            lastProgressLogTime = now;
            logger.info(
                IoTConsensusMessages.SNAPSHOT_TRANSMISSION_PROGRESS,
                newSnapshotDirName,
                transitedFilesNum,
                snapshotPaths.size(),
                humanReadableByteCountSI(transitedSnapshotSizeSum),
                humanReadableByteCountSI(snapshotSizeSum),
                CommonDateTimeUtils.convertMillisecondToDurationStr((now - startTime) / 1_000_000),
                file);
          }
        } finally {
          reader.close();
        }
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.SNAPSHOT_TRANSMISSION_SEND_ERROR, targetPeer), e);
    }
    logger.info(
        IoTConsensusMessages.SNAPSHOT_TRANSMISSION_COMPLETE,
        CommonDateTimeUtils.convertMillisecondToDurationStr(
            (System.nanoTime() - startTime) / 1_000_000),
        snapshotDir);
  }

  public void receiveSnapshotFragment(
      String snapshotId, String originalFilePath, ByteBuffer fileChunk, long fileOffset)
      throws ConsensusGroupModifyPeerException {
    try {
      String targetFilePath = calculateSnapshotPath(snapshotId, originalFilePath);
      File existingFile = getExistingSnapshotFile(targetFilePath);
      if (existingFile != null) {
        writeSnapshotFragment(existingFile, fileChunk, fileOffset);
        return;
      }

      // Place every companion file of the same TsFile into one receive folder. The fileKey rule
      // (filename before the first '.') matches SnapshotLoader so the group stays together. The
      // folder is selected at most once per fileKey via computeIfAbsent, which is safe under the
      // concurrent IoTConsensusRPC-Processor receivers.
      String fileKey = getSnapshotFileKey(targetFilePath);
      ConcurrentHashMap<String, String> folderMap =
          snapshotReceiveFolderMap.computeIfAbsent(snapshotId, k -> new ConcurrentHashMap<>());
      String folder;
      try {
        folder =
            folderMap.computeIfAbsent(
                fileKey,
                k -> {
                  try {
                    return recvFolderManager.getNextFolder();
                  } catch (DiskSpaceInsufficientException ex) {
                    throw new RuntimeException(ex);
                  }
                });
      } catch (RuntimeException re) {
        if (re.getCause() instanceof DiskSpaceInsufficientException) {
          throw (DiskSpaceInsufficientException) re.getCause();
        }
        throw re;
      }
      writeSnapshotFragment(getSnapshotPath(folder, targetFilePath), fileChunk, fileOffset);
    } catch (IOException e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.ERROR_RECEIVING_SNAPSHOT, snapshotId), e);
    } catch (DiskSpaceInsufficientException e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.ERROR_RECEIVING_SNAPSHOT, snapshotId), e);
    }
  }

  private File getExistingSnapshotFile(String targetFilePath) {
    for (String folder : recvFolderManager.getFolders()) {
      File targetFile = getSnapshotPath(folder, targetFilePath);
      if (targetFile.exists()) {
        return targetFile;
      }
    }
    return null;
  }

  private void writeSnapshotFragment(File targetFile, ByteBuffer fileChunk, long fileOffset)
      throws IOException {
    Path parentDir = Paths.get(targetFile.getParent());
    if (!Files.exists(parentDir)) {
      Files.createDirectories(parentDir);
    }
    try (FileOutputStream fos = new FileOutputStream(targetFile.getAbsolutePath(), true);
        FileChannel channel = fos.getChannel()) {
      channel.write(fileChunk.slice(), fileOffset);
    }
  }

  private String calculateSnapshotPath(String snapshotId, String originalFilePath)
      throws ConsensusGroupModifyPeerException {
    if (!originalFilePath.contains(snapshotId)) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.INVALID_SNAPSHOT_FILE, snapshotId, originalFilePath));
    }
    return originalFilePath.substring(originalFilePath.indexOf(snapshotId));
  }

  /**
   * Groups companion files of one TsFile. Uses the same rule as {@code
   * SnapshotLoader#createLinksFromSnapshotToSourceDir}: the file name up to the first {@code '.'}.
   */
  private String getSnapshotFileKey(String targetFilePath) {
    return new File(targetFilePath).getName().split("\\.")[0];
  }

  private void clearOldSnapshot() {
    File directory = new File(storageDir);
    File[] versionFiles = directory.listFiles((dir, name) -> name.startsWith(SNAPSHOT_DIR_NAME));
    if (versionFiles == null || versionFiles.length == 0) {
      logger.error(IoTConsensusMessages.CANNOT_FIND_SNAPSHOT_DIR, thisNode.getGroupId());
      return;
    }
    for (File file : versionFiles) {
      if (!file.getName().equals(newSnapshotDirName)) {
        try {
          FileUtils.deleteDirectory(file);
        } catch (IOException e) {
          logger.error(IoTConsensusMessages.DELETE_OLD_SNAPSHOT_FAILED, file.getAbsolutePath(), e);
        }
      }
    }
  }

  public boolean loadSnapshot(String snapshotId) {
    // Snapshot fragments are spread across the receive folders by the FolderManager (a DataRegion,
    // for example, uses one receive folder per local data dir), so a given snapshot only exists
    // under the folders that actually received fragments. Collect exactly those folders and hand
    // them to the state machine in a single load call.
    //
    // It must be a single call rather than one call per folder: the state machine's load is
    // destructive (a DataRegion load wipes the data dirs before relinking), so loading folders one
    // at a time would make each load erase the fragments linked by the previous folders, leaving
    // only the last folder's data. The state machine instead clears the data dirs once and relinks
    // every folder's fragments together.
    //
    // Note: an empty region produces a snapshot with zero fragments, so none of the receive folders
    // contains it. That is a legitimate (no-op) load, not a failure, so an absent snapshot must not
    // be reported as failure here.
    try {
      List<File> snapshotDirs = new ArrayList<>();
      for (String dir : recvFolderManager.getFolders()) {
        File snapshotDir = getSnapshotPath(dir, snapshotId);
        if (snapshotDir.exists()) {
          snapshotDirs.add(snapshotDir);
        }
      }
      if (snapshotDirs.isEmpty()) {
        return true;
      }
      return stateMachine.loadSnapshot(snapshotDirs);
    } finally {
      // Receiving is finished for this snapshot; drop its receive-folder mapping.
      snapshotReceiveFolderMap.remove(snapshotId);
    }
  }

  private File getSnapshotPath(String curStorageDir, String snapshotRelativePath) {
    File storageDirFile = new File(curStorageDir);
    File snapshotDir = new File(curStorageDir, snapshotRelativePath);
    try {
      if (!snapshotDir
          .getCanonicalFile()
          .toPath()
          .startsWith(storageDirFile.getCanonicalFile().toPath())) {
        throw new IllegalArgumentException(
            IoTConsensusMessages.INVALID_SNAPSHOT_RELATIVE_PATH + snapshotRelativePath);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return snapshotDir;
  }

  @FunctionalInterface
  public interface ThrowableFunction<T, R> {
    R apply(T t) throws Exception;
  }

  public void inactivatePeer(Peer peer, boolean forDeletionPurpose)
      throws ConsensusGroupModifyPeerException {
    ConsensusGroupModifyPeerException lastException = null;
    // In region migration, if the target node restarts before the "addRegionPeer" phase within 1
    // minutes,
    // the client in the ClientManager will become invalid.
    // This PR adds 1 retry at this point to ensure that region migration can still proceed
    // correctly in such cases.
    for (int i = 0; i < 2; i++) {
      try (SyncIoTConsensusServiceClient client =
          syncClientManager.borrowClient(peer.getEndpoint())) {
        try {
          TInactivatePeerRes res =
              client.inactivatePeer(
                  new TInactivatePeerReq(peer.getGroupId().convertToTConsensusGroupId())
                      .setForDeletionPurpose(forDeletionPurpose));
          if (isSuccess(res.status)) {
            return;
          }
          lastException =
              new ConsensusGroupModifyPeerException(
                  String.format(
                      IoTConsensusMessages.ERROR_INACTIVATING_PEER, peer, res.getStatus()));
        } catch (Exception e) {
          lastException =
              new ConsensusGroupModifyPeerException(
                  String.format(IoTConsensusMessages.ERROR_INACTIVATING_PEER_SHORT, peer), e);
        }
      } catch (ClientManagerException e) {
        lastException = new ConsensusGroupModifyPeerException(e);
      }
    }
    throw lastException;
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
            String.format(
                IoTConsensusMessages.ERROR_TRIGGERING_SNAPSHOT_LOAD, peer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.ERROR_ACTIVATING_PEER_SHORT, peer), e);
    }
  }

  public void activePeer(Peer peer) throws ConsensusGroupModifyPeerException {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(peer.getEndpoint())) {
      TActivatePeerRes res =
          client.activatePeer(new TActivatePeerReq(peer.getGroupId().convertToTConsensusGroupId()));
      if (!isSuccess(res.status)) {
        throw new ConsensusGroupModifyPeerException(
            String.format(IoTConsensusMessages.ERROR_ACTIVATING_PEER, peer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.ERROR_ACTIVATING_PEER_SHORT, peer), e);
    }
  }

  public void notifyPeersToBuildSyncLogChannel(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    // The configuration will be modified during iterating because we will add the targetPeer to
    // configuration
    List<Peer> currentMembers = new ArrayList<>(this.configuration);
    logger.info(
        IoTConsensusMessages.NOTIFY_PEERS_BUILD_SYNC_LOG_DETAIL, currentMembers, targetPeer);
    for (Peer peer : currentMembers) {
      logger.info(IoTConsensusMessages.BUILD_SYNC_LOG_CHANNEL_FROM, peer);
      if (peer.equals(thisNode)) {
        // use searchIndex for thisNode as the initialSyncIndex because targetPeer will load the
        // snapshot produced by thisNode
        buildSyncLogChannel(targetPeer, true);
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
                String.format(
                    IoTConsensusMessages.BUILD_SYNC_LOG_CHANNEL_FAILED, peer, targetPeer));
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
              IoTConsensusMessages.LOG_CANNOT_NOTIFY_ARG_BUILD_SYNC_LOG_CHANNEL_490770FB
                  + IoTConsensusMessages.LOG_PLEASE_CHECK_STATUS_NODE_MANUALLY_40FBC9B3,
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
            logger.warn(IoTConsensusMessages.REMOVING_SYNC_LOG_CHANNEL_FAILED, peer, targetPeer);
          }
        } catch (Exception e) {
          logger.warn(
              IoTConsensusMessages.EXCEPTION_REMOVING_SYNC_LOG_CHANNEL, peer, targetPeer, e);
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
              IoTConsensusMessages.WAIT_SYNC_LOG_COMPLETED,
              targetPeer,
              res.searchIndex,
              res.safeIndex);
          return;
        }
        logger.info(
            IoTConsensusMessages.WAIT_SYNC_LOG_IN_PROGRESS,
            targetPeer,
            res.searchIndex,
            res.safeIndex);
        Thread.sleep(checkIntervalInMs);
      }
    } catch (ClientManagerException | TException e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(
              IoTConsensusMessages.ERROR_WAITING_SYNC_LOG_COMPLETE, targetPeer, e.getMessage()),
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format(
              IoTConsensusMessages.THREAD_INTERRUPTED_WAITING_SYNC_LOG, targetPeer, e.getMessage()),
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
          logger.info(ConsensusMessages.WAIT_RELEASE_HAS_RELEASED, targetPeer);
          return;
        }
        logger.info(ConsensusMessages.WAIT_RELEASE_STILL_RELEASING, targetPeer);
        Thread.sleep(checkIntervalInMs);
      }
    } catch (ClientManagerException | TException e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(
              ConsensusMessages.ERROR_WAITING_RELEASE_RESOURCE, targetPeer, e.getMessage()),
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format(
              ConsensusMessages.THREAD_INTERRUPTED_WAITING_RELEASE_RESOURCE,
              targetPeer,
              e.getMessage()),
          e);
    }
  }

  private boolean isSuccess(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private TSStatus syncWriterSafeTimeBarrierToPeer(
      final Peer targetPeer,
      final long safePhysicalTime,
      final int writerNodeId,
      final long barrierLocalSeq) {
    try (SyncIoTConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      final TSyncWriterSafeTimeBarrierRes res =
          client.syncWriterSafeTimeBarrier(
              new TSyncWriterSafeTimeBarrierReq()
                  .setConsensusGroupId(thisNode.getGroupId().convertToTConsensusGroupId())
                  .setSafePhysicalTime(safePhysicalTime)
                  .setWriterNodeId(writerNodeId)
                  .setBarrierLocalSeq(barrierLocalSeq));
      return res.getStatus();
    } catch (Exception e) {
      logger.debug(
          IoTConsensusMessages.LOG_FAILED_SYNC_WRITER_SAFE_TIME_BARRIER_PEER_ARG_GROUP_ARG_05A13E6A
              + IoTConsensusMessages.LOG_SAFEPT_ARG_WRITERNODEID_ARG_BARRIER_ARG_AC74618F,
          targetPeer,
          consensusGroupId,
          safePhysicalTime,
          writerNodeId,
          barrierLocalSeq,
          e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  /** build SyncLog channel with safeIndex as the default initial sync index. */
  public void buildSyncLogChannel(Peer targetPeer, boolean startNow) {
    buildSyncLogChannel(targetPeer, getMinSyncIndex(), startNow);
  }

  public void buildSyncLogChannel(Peer targetPeer, long initialSyncIndex, boolean startNow) {
    KillPoint.setKillPoint(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE);
    configuration.add(targetPeer);
    if (Objects.equals(targetPeer, thisNode)) {
      return;
    }
    logDispatcher.addLogDispatcherThread(targetPeer, initialSyncIndex, startNow);
    logger.info(
        IoTConsensusMessages.BUILD_SYNC_LOG_CHANNEL_SUCCESS,
        targetPeer,
        initialSyncIndex,
        startNow
            ? IoTConsensusMessages.SYNC_LOG_CHANNEL_STARTED
            : IoTConsensusMessages.SYNC_LOG_CHANNEL_START_LATER);
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
      logger.info(IoTConsensusMessages.LOG_DISPATCHER_REMOVED_CLEANUP, targetPeer);
    } catch (Exception e) {
      logger.warn(IoTConsensusMessages.EXCEPTION_REMOVING_LOG_DISPATCHER, e);
      suggestion = IoTConsensusMessages.SUGGEST_RESTART_DATANODE;
      exceptionHappened = true;
    }
    if (!exceptionHappened) {
      logger.info(IoTConsensusMessages.LOG_DISPATCHER_REMOVED_AND_CLEANUP, targetPeer);
    }
    // step 2, update configuration
    configuration.remove(targetPeer);
    checkAndUpdateSafeDeletedSearchIndex();
    logger.info(IoTConsensusMessages.CONFIGURATION_UPDATED, this.configuration, suggestion);
    return !exceptionHappened;
  }

  public static String generateConfigurationDatFileName(int nodeId, String suffix) {
    return nodeId + "_" + suffix;
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForLocalRequest(
      IConsensusRequest request) {
    if (request instanceof ComparableConsensusRequest) {
      final IoTProgressIndex iotProgressIndex =
          new IoTProgressIndex(thisNode.getNodeId(), searchIndex.get() + 1);
      ((ComparableConsensusRequest) request).setProgressIndex(iotProgressIndex);
    }
    return new IndexedConsensusRequest(searchIndex.get() + 1, Collections.singletonList(request))
        .setPhysicalTime(assignPhysicalTimeInMs())
        .setNodeId(thisNode.getNodeId());
  }

  public IndexedConsensusRequest buildIndexedConsensusRequestForRemoteRequest(
      long syncIndex,
      long routingEpoch,
      long physicalTime,
      int nodeId,
      List<IConsensusRequest> requests) {
    observePhysicalTimeLowerBound(physicalTime);
    IndexedConsensusRequest req =
        new IndexedConsensusRequest(ConsensusReqReader.DEFAULT_SEARCH_INDEX, syncIndex, requests);
    req.setRoutingEpoch(routingEpoch);
    req.setPhysicalTime(physicalTime);
    req.setNodeId(nodeId);
    return req;
  }

  public TSStatus syncIdleWriterSafeTimeBarrierToPeer(final Peer targetPeer) {
    final long safePhysicalTime = assignPhysicalTimeInMs();
    final long safeLocalSeq = searchIndex.get();
    writerSafeFrontierTracker.recordAppliedProgress(
        safePhysicalTime, thisNode.getNodeId(), safeLocalSeq);
    return syncWriterSafeTimeBarrierToPeer(
        targetPeer, safePhysicalTime, thisNode.getNodeId(), safeLocalSeq);
  }

  public void observeRemoteWriterSafeTimeBarrier(
      final long safePhysicalTime, final int writerNodeId, final long barrierLocalSeq) {
    observePhysicalTimeLowerBound(safePhysicalTime);
    writerSafeFrontierTracker.observePendingWriterSafeTimeBarrier(
        safePhysicalTime, writerNodeId, barrierLocalSeq);
  }

  public void recordRemoteAppliedWriterProgress(
      final long physicalTime, final int writerNodeId, final long appliedLocalSeq) {
    writerSafeFrontierTracker.recordAppliedProgress(physicalTime, writerNodeId, appliedLocalSeq);
  }

  public long getEffectiveSafePhysicalTime(final int writerNodeId) {
    return writerSafeFrontierTracker.getEffectiveSafePt(writerNodeId);
  }

  public WriterSafeFrontierTracker getWriterSafeFrontierTracker() {
    return writerSafeFrontierTracker;
  }

  public boolean hasSubscriptionConsumers() {
    return !subscriptionQueueRegistry.isEmpty();
  }

  private long assignPhysicalTimeInMs() {
    while (true) {
      final long previous = lastAssignedPhysicalTime.get();
      final long candidate = Math.max(System.currentTimeMillis(), previous);
      if (lastAssignedPhysicalTime.compareAndSet(previous, candidate)) {
        return candidate;
      }
    }
  }

  private void observePhysicalTimeLowerBound(final long observedPhysicalTime) {
    if (observedPhysicalTime <= 0) {
      return;
    }
    while (true) {
      final long previous = lastAssignedPhysicalTime.get();
      final long candidate = Math.max(previous, observedPhysicalTime);
      if (candidate == previous || lastAssignedPhysicalTime.compareAndSet(previous, candidate)) {
        return;
      }
    }
  }

  private void initializeWriterMeta() {
    final long recoveredSearchIndex = searchIndex.get();
    try {
      final Optional<WriterMeta> writerMetaOptional = WriterMeta.load(writerMetaPath);
      if (writerMetaOptional.isPresent()) {
        final WriterMeta writerMeta = writerMetaOptional.get();
        logger.info(
            IoTConsensusMessages
                    .LOG_RECOVERED_WRITER_META_GROUP_ARG_ARG_RECOVEREDLOCALSEQ_ARG_9B989AAC
                + IoTConsensusMessages.LOG_PERSISTEDLOCALSEQ_ARG_5EBAA9A5,
            consensusGroupId,
            writerMetaPath,
            recoveredSearchIndex,
            writerMeta.getLastAllocatedLocalSeq());
        lastAssignedPhysicalTime.set(
            Math.max(writerMeta.getLastAssignedPhysicalTimeMs(), System.currentTimeMillis()));
        latestWriterMeta = writerMeta;
        lastPersistedWriterLocalSeq = writerMeta.getLastAllocatedLocalSeq();
        lastPersistedWriterPhysicalTime = writerMeta.getLastAssignedPhysicalTimeMs();
        return;
      }
    } catch (IOException e) {
      logger.warn(
          IoTConsensusMessages
              .LOG_FAILED_LOAD_WRITER_META_GROUP_ARG_ARG_STARTING_FRESH_WRITER_A24EDEFE,
          consensusGroupId,
          writerMetaPath,
          e);
    }
    lastAssignedPhysicalTime.set(System.currentTimeMillis());
    logger.info(
        IoTConsensusMessages
            .LOG_INITIALIZED_FRESH_WRITER_META_GROUP_ARG_RECOVEREDLOCALSEQ_ARG_A7254C6E,
        consensusGroupId,
        recoveredSearchIndex);
  }

  private void updateWriterMetaOnSuccess(final IndexedConsensusRequest indexedConsensusRequest) {

    persistLatestWriterMeta(false);
  }

  private void persistLatestWriterMeta(final boolean force) {
    final WriterMeta writerMeta = latestWriterMeta;
    if (writerMeta == null) {
      return;
    }
    final long currentTimeMs = System.currentTimeMillis();
    if (!force
        && currentTimeMs - lastWriterMetaPersistAttemptTimeMs
            < WRITER_META_PERSIST_MIN_INTERVAL_MS) {
      return;
    }

    synchronized (writerMetaPersistLock) {
      final WriterMeta latestMeta = latestWriterMeta;
      if (latestMeta == null
          || (latestMeta.getLastAllocatedLocalSeq() <= lastPersistedWriterLocalSeq
              && latestMeta.getLastAssignedPhysicalTimeMs() <= lastPersistedWriterPhysicalTime)) {
        return;
      }

      final long nowMs = System.currentTimeMillis();
      if (!force
          && nowMs - lastWriterMetaPersistAttemptTimeMs < WRITER_META_PERSIST_MIN_INTERVAL_MS) {
        return;
      }

      lastWriterMetaPersistAttemptTimeMs = nowMs;
      try {
        latestMeta.persist(writerMetaPath);
        lastPersistedWriterLocalSeq = latestMeta.getLastAllocatedLocalSeq();
        lastPersistedWriterPhysicalTime = latestMeta.getLastAssignedPhysicalTimeMs();
      } catch (IOException e) {
        logger.warn(
            IoTConsensusMessages
                .LOG_FAILED_PERSIST_WRITER_META_GROUP_ARG_AT_LOCALSEQ_ARG_PT_3502F119,
            consensusGroupId,
            latestMeta.getLastAllocatedLocalSeq(),
            latestMeta.getLastAssignedPhysicalTimeMs(),
            e);
      }
    }
  }

  /**
   * In the case of multiple copies, the minimum synchronization index is selected. In the case of
   * single copies, the current index is selected
   */
  public long getMinSyncIndex() {
    return logDispatcher.getMinSyncIndex().orElseGet(searchIndex::get);
  }

  public long getMinFlushedSyncIndex() {
    return logDispatcher.getMinFlushedSyncIndex().orElseGet(searchIndex::get);
  }

  public String getStorageDir() {
    return storageDir;
  }

  public Peer getThisNode() {
    return thisNode;
  }

  public List<Peer> getConfiguration() {
    return new ArrayList<>(configuration);
  }

  public long getSearchIndex() {
    return searchIndex.get();
  }

  public ConsensusReqReader getConsensusReqReader() {
    return consensusReqReader;
  }

  /**
   * Registers a subscription pending queue for real-time in-memory data delivery. When {@link
   * #write(IConsensusRequest)} succeeds, the IndexedConsensusRequest is offered to all registered
   * subscription queues, enabling subscription consumers to receive data without waiting for WAL
   * flush.
   *
   * @param queue the blocking queue to receive IndexedConsensusRequest entries
   */
  public void registerSubscriptionQueue(
      final BlockingQueue<IndexedConsensusRequest> queue,
      final SubscriptionWalRetentionPolicy retentionPolicy) {
    subscriptionQueueRegistry.register(queue, retentionPolicy);
    // Immediately re-evaluate the safe delete index with new subscription awareness
    checkAndUpdateSafeDeletedSearchIndex();
    logger.info(
        IoTConsensusMessages.LOG_REGISTERED_SUBSCRIPTION_QUEUE_GROUP_ARG_5102ABA0
            + IoTConsensusMessages
                .LOG_TOTAL_SUBSCRIPTION_QUEUES_ARG_CURRENTSEARCHINDEX_ARG_ARG_9BF9006A,
        consensusGroupId,
        subscriptionQueueRegistry.size(),
        searchIndex.get(),
        System.identityHashCode(this));
  }

  public void deregisterSubscriptionQueue(final BlockingQueue<IndexedConsensusRequest> queue) {
    subscriptionQueueRegistry.unregister(queue);
    // Re-evaluate: with fewer subscribers, more WAL may be deletable
    checkAndUpdateSafeDeletedSearchIndex();
    logger.info(
        IoTConsensusMessages
            .LOG_DEREGISTERED_SUBSCRIPTION_QUEUE_GROUP_ARG_REMAINING_SUBSCRIPTION_QUEUES_ARG_B86E31AF,
        consensusGroupId,
        subscriptionQueueRegistry.size());
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
    logger.info(ConsensusMessages.SET_ACTIVE_STATUS, this.thisNode, active);
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
                IoTConsensusMessages.CLEANUP_REMOTE_SNAPSHOT_FAILED, targetPeer, res.getStatus()));
      }
    } catch (Exception e) {
      throw new ConsensusGroupModifyPeerException(
          String.format(IoTConsensusMessages.CLEANUP_REMOTE_SNAPSHOT_FAILED_SHORT, targetPeer), e);
    }
  }

  public void cleanupSnapshot(String snapshotId) throws ConsensusGroupModifyPeerException {
    snapshotReceiveFolderMap.remove(snapshotId);
    List<String> allDirs = new ArrayList<>(Collections.singletonList(storageDir));
    allDirs.addAll(recvFolderManager.getFolders());
    for (String dir : allDirs) {
      File snapshotDir = getSnapshotPath(dir, snapshotId);
      if (snapshotDir.exists()) {
        try {
          FileUtils.deleteDirectory(snapshotDir);
        } catch (IOException e) {
          throw new ConsensusGroupModifyPeerException(e);
        }
      } else {
        logger.info(IoTConsensusMessages.FILE_NOT_EXIST, snapshotDir);
      }
    }
  }

  public void cleanupLocalSnapshot() {
    try {
      cleanupSnapshot(newSnapshotDirName);
      stateMachine.clearSnapshot();
    } catch (ConsensusGroupModifyPeerException e) {
      logger.warn(IoTConsensusMessages.CLEANUP_LOCAL_SNAPSHOT_FAIL, newSnapshotDirName, e);
    }
  }

  void checkAndUpdateIndex() {
    // Since the underlying wal does not persist safelyDeletedSearchIndex, IoTConsensus needs to
    // update wal with its syncIndex recovered from the consensus layer when initializing.
    // This prevents wal from being piled up if the safelyDeletedSearchIndex is not updated after
    // the restart and Leader migration occurs
    checkAndUpdateSafeDeletedSearchIndex();
    // see message in logs for details
    checkAndUpdateSearchIndex();
  }

  /**
   * Computes and updates the safe-to-delete WAL search index based on replication progress and
   * subscription WAL retention policy.
   *
   * <p>Because multiple subscription topics share one region WAL, the effective per-region
   * retention policy is the most conservative policy across all active subscription queues on this
   * region. Retention is applied at rolled WAL-file granularity and may therefore lag behind the
   * configured thresholds.
   */
  public void checkAndUpdateSafeDeletedSearchIndex() {
    if (configuration.isEmpty()) {
      logger.error(IoTConsensusMessages.CONFIGURATION_EMPTY_UNEXPECTED);
      return;
    }

    final boolean hasSubscriptions = !subscriptionQueueRegistry.isEmpty();

    if (configuration.size() == 1 && !hasSubscriptions) {
      // Single replica, no subscription consumers => delete all WAL freely
      consensusReqReader.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
      consensusReqReader.setSubscriptionRetainedMinVersionId(Long.MAX_VALUE);
      return;
    }

    final long replicationIndex =
        configuration.size() > 1 ? getMinFlushedSyncIndex() : Long.MAX_VALUE;

    final SubscriptionRetentionBound subscriptionRetentionBound =
        subscriptionWalRetentionCalculator.calculate(
            subscriptionQueueRegistry.getRetentionPolicies());

    consensusReqReader.setSafelyDeletedSearchIndex(
        Math.min(replicationIndex, subscriptionRetentionBound.getSafelyDeletedSearchIndex()));
    consensusReqReader.setSubscriptionRetainedMinVersionId(
        subscriptionRetentionBound.getRetainedMinVersionId());
  }

  public void checkAndUpdateSearchIndex() {
    long currentSearchIndex = searchIndex.get();
    long safelyDeletedSearchIndex = getMinFlushedSyncIndex();
    if (currentSearchIndex < safelyDeletedSearchIndex) {
      logger.warn(
          IoTConsensusMessages.SEARCH_INDEX_SMALLER_THAN_SAFELY_DELETED,
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
          IoTConsensusMessages.CACHE_AND_INSERT_START,
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
                  IoTConsensusMessages.WAITING_TARGET_REQUEST_TIMEOUT,
                  request.getStartSyncIndex(),
                  nextSyncIndex);
              requestCache.remove(request);
              nextSyncIndex = Math.max(nextSyncIndex, request.getEndSyncIndex() + 1);
              break;
            }
          } catch (InterruptedException e) {
            logger.warn(
                IoTConsensusMessages.CURRENT_WAITING_INTERRUPTED, request.getStartSyncIndex(), e);
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
        if (subStatus.stream()
            .allMatch(status -> status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
          recordRemoteAppliedWriterProgress(
              request.getEndPhysicalTime(), request.getWriterNodeId(), request.getEndSyncIndex());
        }
        long applyTime = System.nanoTime();
        ioTConsensusServerMetrics.recordApplyCost(applyTime - sortTime);
        queueSortCondition.signalAll();
        logger.debug(
            IoTConsensusMessages.CACHE_AND_INSERT_END,
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
