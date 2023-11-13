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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.queryengine.execution.load.locseq.LocationSequencer;
import org.apache.iotdb.db.queryengine.execution.load.locseq.LocationStatistics;
import org.apache.iotdb.db.queryengine.execution.load.locseq.ThroughputBasedLocationSequencer;
import org.apache.iotdb.db.queryengine.execution.load.nodesplit.ClusteringMeasurementSplitter;
import org.apache.iotdb.db.queryengine.execution.load.nodesplit.PieceNodeSplitter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileDispatcherImpl.NODE_CONNECTION_ERROR;

public class TsFileSplitSender {

  private static final int MAX_RETRY = 5;
  private static final long RETRY_INTERVAL_MS = 6_000L;
  private static final int MAX_PENDING_PIECE_NODE = 5;
  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitSender.class);

  private LoadTsFileNode loadTsFileNode;
  private DataPartitionBatchFetcher targetPartitionFetcher;
  private long targetPartitionInterval;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;
  // All consensus groups accessed in Phase1 should be notified in Phase2
  private final Set<TRegionReplicaSet> allReplicaSets = new ConcurrentSkipListSet<>();
  private String uuid;
  private LocationStatistics locationStatistics = new LocationStatistics();
  private boolean isGeneratedByPipe;
  private Map<Pair<LoadTsFilePieceNode, TRegionReplicaSet>, Exception> phaseOneFailures =
      new ConcurrentHashMap<>();
  private Map<TConsensusGroupId, Exception> phaseTwoFailures = new HashMap<>();
  private long maxSplitSize;
  private PieceNodeSplitter pieceNodeSplitter = new ClusteringMeasurementSplitter(1.0, 10);
  //        private PieceNodeSplitter pieceNodeSplitter = new OrderedMeasurementSplitter();
  private CompressionType compressionType = CompressionType.LZ4;
  private Statistic statistic = new Statistic();
  private ExecutorService splitNodeService;
  private Queue<Pair<Future<List<LoadTsFilePieceNode>>, TRegionReplicaSet>> splitFutures;
  private int maxConcurrentFileNum;
  private String userName;
  private String password;

  public TsFileSplitSender(
      LoadTsFileNode loadTsFileNode,
      DataPartitionBatchFetcher targetPartitionFetcher,
      long targetPartitionInterval,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      boolean isGeneratedByPipe,
      long maxSplitSize,
      int maxConcurrentFileNum,
      String userName,
      String password) {
    this.loadTsFileNode = loadTsFileNode;
    this.targetPartitionFetcher = targetPartitionFetcher;
    this.targetPartitionInterval = targetPartitionInterval;
    this.internalServiceClientManager = internalServiceClientManager;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.maxSplitSize = maxSplitSize;
    this.splitNodeService = IoTDBThreadPoolFactory.newCachedThreadPool("SplitLoadTsFilePieceNode");
    this.splitFutures = new ArrayDeque<>(MAX_PENDING_PIECE_NODE);
    this.maxConcurrentFileNum = maxConcurrentFileNum;
    this.userName = userName;
    this.password = password;

    this.statistic.totalSize = loadTsFileNode.getTotalSize();
  }

  public void start() throws IOException {
    statistic.taskStartTime = System.currentTimeMillis();
    // skip files without data
    loadTsFileNode.getResources().removeIf(f -> f.getDevices().isEmpty());
    uuid = UUID.randomUUID().toString();
    logger.info("Start to split {}", loadTsFileNode);

    boolean isFirstPhaseSuccess = firstPhase(loadTsFileNode);
    boolean isSecondPhaseSuccess = secondPhase(isFirstPhaseSuccess);
    if (isFirstPhaseSuccess && isSecondPhaseSuccess) {
      logger.info("Load TsFiles {} Successfully", loadTsFileNode.getResources());
    } else {
      logger.warn("Can not Load TsFiles {}", loadTsFileNode.getResources());
    }
    statistic.taskEndTime = System.currentTimeMillis();
    locationStatistics.logLocationStatistics();
    statistic.logStatistic();
  }

  private boolean firstPhase(LoadTsFileNode node) throws IOException {
    long start = System.currentTimeMillis();
    TsFileDataManager tsFileDataManager =
        new DeviceBatchTsFileDataManager(
            this::dispatchOnePieceNode,
            node.getPlanNodeId(),
            node.lastResource().getTsFile(),
            targetPartitionFetcher,
            maxSplitSize,
            userName);

    ExecutorService executorService =
        IoTDBThreadPoolFactory.newThreadPool(
            32,
            Integer.MAX_VALUE,
            20,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new IoTThreadFactory("MergedTsFileSplitter"),
            "MergedTsFileSplitter");
    MergedTsFileSplitter splitter =
        new MergedTsFileSplitter(
            node.getResources().stream()
                .map(TsFileResource::getTsFile)
                .collect(Collectors.toList()),
            tsFileDataManager::addOrSendTsFileData,
            executorService,
            targetPartitionInterval,
            maxConcurrentFileNum);
    splitter.splitTsFileByDataPartition();
    splitter.close();
    logger.info("Split ends after {}ms", System.currentTimeMillis() - start);
    boolean success =
        tsFileDataManager.sendAllTsFileData()
            && processRemainingPieceNodes()
            && phaseOneFailures.isEmpty();
    statistic.p1TimeMS = System.currentTimeMillis() - start;
    logger.info("Cleanup ends after {}ms", statistic.p1TimeMS);
    return success;
  }

  private Void loadInGroup(
      TRegionReplicaSet replicaSet, TLoadCommandReq loadCommandReq, AtomicBoolean hasTimeout)
      throws SocketException {
    Exception locationException = null;
    for (TDataNodeLocation dataNodeLocation : replicaSet.dataNodeLocations) {
      logger.info(
          "Start dispatching Load command for uuid {} to {} of {}",
          uuid,
          dataNodeLocation,
          replicaSet.regionId);
      TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
      boolean loaded = false;

      for (int i = 0; i < MAX_RETRY; i++) {
        try (SyncDataNodeInternalServiceClient client =
            internalServiceClientManager.borrowClient(endPoint)) {
          // record timeout for recalculating max batch size
          if (statistic.p2Timeout == 0) {
            statistic.p2Timeout = client.getTimeout();
          }

          TLoadResp loadResp = client.sendLoadCommand(loadCommandReq);
          if (!loadResp.isAccepted()) {
            logger.warn(loadResp.message);
            locationException = new FragmentInstanceDispatchException(loadResp.status);
          } else {
            // if any node in this replica set succeeds, it is loaded
            locationException = null;
            loaded = true;
          }
          break;
        } catch (ClientManagerException | TException e) {
          TSStatus status = new TSStatus();
          status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
          status.setMessage(
              "can't connect to node {}, please reset longer dn_connection_timeout_ms "
                  + "in iotdb-common.properties and restart iotdb."
                  + endPoint);
          locationException = new FragmentInstanceDispatchException(status);
          hasTimeout.set(true);
        }

        try {
          Thread.sleep(RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
          locationException = e;
          break;
        }
      }

      if (loaded) {
        // if any node in this replica set succeeds, it is loaded
        break;
      }
    }

    if (locationException != null) {
      phaseTwoFailures.put(replicaSet.regionId, locationException);
    }
    return null;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess) {
    TLoadCommandReq loadCommandReq =
        new TLoadCommandReq(
            (isFirstPhaseSuccess ? LoadCommand.EXECUTE : LoadCommand.ROLLBACK).ordinal(), uuid);
    loadCommandReq.setIsGeneratedByPipe(isGeneratedByPipe);

    long p2StartMS = System.currentTimeMillis();
    List<Pair<TRegionReplicaSet, Future<Void>>> loadFutures = new ArrayList<>();
    AtomicBoolean hasTimeout = new AtomicBoolean();
    for (TRegionReplicaSet replicaSet : allReplicaSets) {
      loadFutures.add(
          new Pair<>(
              replicaSet,
              splitNodeService.submit(() -> loadInGroup(replicaSet, loadCommandReq, hasTimeout))));
    }
    for (Pair<TRegionReplicaSet, Future<Void>> loadFuture : loadFutures) {
      try {
        loadFuture.right.get();
      } catch (InterruptedException | ExecutionException e) {
        phaseTwoFailures.put(loadFuture.left.regionId, e);
      }
    }
    statistic.p2TimeMS = System.currentTimeMillis() - p2StartMS;
    statistic.hasP2Timeout = hasTimeout.get();

    return phaseTwoFailures.isEmpty();
  }

  public LocationSequencer createLocationSequencer(TRegionReplicaSet replicaSet) {
    //    return new FixedLocationSequencer(replicaSet);
    //    return new RandomLocationSequencer(replicaSet);
    return new ThroughputBasedLocationSequencer(replicaSet, locationStatistics);
  }

  private ByteBuffer compressBuffer(ByteBuffer buffer) throws IOException {
    statistic.rawSize.addAndGet(buffer.remaining());
    if (compressionType.equals(CompressionType.UNCOMPRESSED)) {
      statistic.compressedSize.addAndGet(buffer.remaining());
      return buffer;
    }
    ICompressor compressor = ICompressor.getCompressor(compressionType);
    int maxBytesForCompression = compressor.getMaxBytesForCompression(buffer.remaining()) + 1;
    ByteBuffer compressed = ByteBuffer.allocate(maxBytesForCompression);
    int compressLength =
        compressor.compress(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            buffer.remaining(),
            compressed.array());
    compressed.limit(compressLength);
    statistic.compressedSize.addAndGet(compressLength);
    return compressed;
  }

  private Future<List<LoadTsFilePieceNode>> submitSplitPieceNode(LoadTsFilePieceNode pieceNode) {
    return splitNodeService.submit(() -> pieceNodeSplitter.split(pieceNode));
  }

  private boolean processRemainingPieceNodes() {
    List<LoadTsFilePieceNode> subNodes;
    for (Pair<Future<List<LoadTsFilePieceNode>>, TRegionReplicaSet> pair : splitFutures) {
      try {
        subNodes = pair.left.get();
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Unexpected error during splitting node", e);
        return false;
      }
      if (!dispatchPieceNodes(subNodes, pair.right)) {
        return false;
      }
    }
    return true;
  }

  private TTsFilePieceReq genLoadReq(
      ByteBuffer buffer, TRegionReplicaSet replicaSet, int uncompressedLength) {
    TTsFilePieceReq loadTsFileReq = new TTsFilePieceReq(buffer, uuid, replicaSet.getRegionId());
    loadTsFileReq.setUsername(userName);
    loadTsFileReq.setPassword(password);
    loadTsFileReq.setCompressionType(compressionType.serialize());
    loadTsFileReq.setUncompressedLength(uncompressedLength);
    return loadTsFileReq;
  }

  private boolean dispatchOneFinalNode(
      TTsFilePieceReq loadTsFileReq, TRegionReplicaSet replicaSet, TDataNodeLocation location)
      throws Exception {
    TRegionReplicaSet relaySet = new TRegionReplicaSet(replicaSet);
    relaySet.getDataNodeLocations().remove(location);
    loadTsFileReq.setRelayTargets(relaySet);
    loadTsFileReq.setNeedSchemaRegistration(true);
    Exception lastConnectionError = null;

    if (location.getDataNodeId() == 0 && logger.isDebugEnabled()) {
      locationStatistics.logLocationStatistics();
      logger.debug("Chose location {}", location.getDataNodeId());
    }
    for (int i = 0; i < MAX_RETRY; i++) {
      try (SyncDataNodeInternalServiceClient client =
          internalServiceClientManager.borrowClient(location.internalEndPoint)) {
        TLoadResp loadResp = client.sendTsFilePieceNode(loadTsFileReq);
        logger.debug("Response from {}: {}", location.getDataNodeId(), loadResp);
        if (!loadResp.isAccepted()) {
          logger.warn(loadResp.message);
          throw new FragmentInstanceDispatchException(loadResp.status);
        }
        return true;
      } catch (ClientManagerException | TException e) {
        lastConnectionError = e;
      }

      try {
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException e) {
        return false;
      }
    }
    throw lastConnectionError;
  }

  private boolean dispatchOneFinalNode(LoadTsFilePieceNode node, TRegionReplicaSet replicaSet) {
    ByteBuffer buffer;
    long startTime = System.nanoTime();
    int uncompressedLength;
    try {
      buffer = node.serializeToByteBuffer();
      uncompressedLength = buffer.remaining();
      buffer = compressBuffer(buffer);
    } catch (IOException e) {
      phaseOneFailures.put(new Pair<>(node, replicaSet), e);
      return false;
    }
    long compressingTime = System.nanoTime() - startTime;
    statistic.compressingTimeNs.addAndGet(compressingTime);

    TTsFilePieceReq loadTsFileReq = genLoadReq(buffer, replicaSet, uncompressedLength);
    LocationSequencer locationSequencer = createLocationSequencer(replicaSet);

    boolean loadSucceed = false;
    Exception lastConnectionError = null;
    TDataNodeLocation currLocation = null;
    for (TDataNodeLocation location : locationSequencer) {
      currLocation = location;
      startTime = System.nanoTime();
      try {
        loadSucceed = dispatchOneFinalNode(loadTsFileReq, replicaSet, currLocation);
      } catch (FragmentInstanceDispatchException e) {
        phaseOneFailures.put(new Pair<>(node, replicaSet), e);
        return false;
      } catch (Exception e) {
        if (lastConnectionError != null) {
          logger.debug("Multiple connection error occurred, previous one:", lastConnectionError);
        }
        lastConnectionError = e;
      }
      if (loadSucceed) {
        break;
      }
    }

    if (!loadSucceed) {
      String warning = NODE_CONNECTION_ERROR;
      logger.warn(warning, currLocation, lastConnectionError);
      TSStatus status = new TSStatus();
      status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      status.setMessage(warning + currLocation);
      phaseOneFailures.put(
          new Pair<>(node, replicaSet), new FragmentInstanceDispatchException(status));
      return false;
    }
    long timeConsumption = System.nanoTime() - startTime;
    logger.debug("Time consumption: {}", timeConsumption);
    locationStatistics.updateThroughput(currLocation, node.getDataSize(), timeConsumption);

    return true;
  }

  private boolean dispatchPieceNodes(
      List<LoadTsFilePieceNode> subNodes, TRegionReplicaSet replicaSet) {

    long start = System.nanoTime();
    List<Boolean> subNodeResults =
        subNodes.stream()
            .parallel()
            .map(node -> dispatchOneFinalNode(node, replicaSet))
            .collect(Collectors.toList());
    long elapsedTime = System.nanoTime() - start;
    statistic.dispatchNodesTimeNS.addAndGet(elapsedTime);
    return !subNodeResults.contains(false);
  }

  public boolean dispatchOnePieceNode(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    long allStart = System.nanoTime();
    // determine which replicas should receive the P2 message
    allReplicaSets.add(replicaSet);

    List<LoadTsFilePieceNode> subNodes;
    // split the piece node asynchronously to improve parallelism
    if (splitFutures.size() < MAX_PENDING_PIECE_NODE) {
      splitFutures.add(new Pair<>(submitSplitPieceNode(pieceNode), replicaSet));
      statistic.dispatchNodeTimeNS.addAndGet(System.nanoTime() - allStart);
      return true;
    } else {
      // wait for the first split task to complete if too many task
      long start = System.nanoTime();
      Pair<Future<List<LoadTsFilePieceNode>>, TRegionReplicaSet> pair = splitFutures.poll();
      try {
        subNodes = pair.left.get();
        long elapsedTime = System.nanoTime() - start;
        statistic.splitTime.addAndGet(elapsedTime);
        statistic.pieceNodeNum.incrementAndGet();
        logger.debug(
            "{} splits are generated after {}ms", subNodes.size(), elapsedTime / 1_000_000L);

        splitFutures.add(new Pair<>(submitSplitPieceNode(pieceNode), replicaSet));
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Unexpected error during splitting node", e);
        return false;
      }
      // send the split nodes to the replicas
      boolean success = dispatchPieceNodes(subNodes, pair.right);
      statistic.dispatchNodeTimeNS.addAndGet(System.nanoTime() - allStart);
      return success;
    }
  }

  public static class Statistic {

    public long taskStartTime;
    public long taskEndTime;
    public AtomicLong rawSize = new AtomicLong();
    public AtomicLong compressedSize = new AtomicLong();
    public AtomicLong splitTime = new AtomicLong();
    public AtomicLong pieceNodeNum = new AtomicLong();
    public AtomicLong dispatchNodesTimeNS = new AtomicLong();
    public AtomicLong dispatchNodeTimeNS = new AtomicLong();
    public AtomicLong compressingTimeNs = new AtomicLong();
    public long p1TimeMS;
    public long p2TimeMS;
    public long totalSize;
    public boolean hasP2Timeout;
    public long p2Timeout;

    public void logStatistic() {
      logger.info(
          "Time consumption: {}ms, totalSize: {}MB",
          taskEndTime - taskStartTime,
          totalSize * 1.0 / MB);
      logger.info(
          "Generated {} piece nodes, splitTime: {}, dispatchSplitsTime: {}, dispatchNodeTime: {}",
          pieceNodeNum.get(),
          splitTime.get() / 1_000_000L,
          dispatchNodesTimeNS.get() / 1_000_000L,
          dispatchNodeTimeNS.get() / 1_000_000L);
      logger.info(
          "Transmission size: {}/{} ({}), compressionTime: {}ms",
          compressedSize.get(),
          rawSize.get(),
          compressedSize.get() * 1.0 / rawSize.get(),
          compressingTimeNs.get() / 1_000_000L);
      logger.info("Sync TsFile time: {}ms ({})", p1TimeMS, p1ThroughputMBPS());
      logger.info("Load command execution time: {}ms ({})", p2TimeMS, p2ThroughputMBPS());
    }

    public double p2ThroughputMBPS() {
      return totalSize * 1.0 / MB / (p2TimeMS / 1000.0);
    }

    public double p1ThroughputMBPS() {
      return totalSize * 1.0 / MB / (p1TimeMS / 1000.0);
    }
  }

  public Statistic getStatistic() {
    return statistic;
  }
}
