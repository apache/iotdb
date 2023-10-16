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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
  private Map<TRegionReplicaSet, Exception> phaseTwoFailures = new HashMap<>();
  private long maxSplitSize;
  private PieceNodeSplitter pieceNodeSplitter = new ClusteringMeasurementSplitter(1.0, 10);
  //        private PieceNodeSplitter pieceNodeSplitter = new OrderedMeasurementSplitter();
  private CompressionType compressionType = CompressionType.LZ4;
  private Statistic statistic = new Statistic();
  private ExecutorService splitNodeService;
  private Queue<Pair<Future<List<LoadTsFilePieceNode>>, TRegionReplicaSet>> splitFutures;
  private int maxConcurrentFileNum;
  private String userName;

  public TsFileSplitSender(
      LoadTsFileNode loadTsFileNode,
      DataPartitionBatchFetcher targetPartitionFetcher,
      long targetPartitionInterval,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      boolean isGeneratedByPipe,
      long maxSplitSize,
      int maxConcurrentFileNum,
      String userName) {
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
  }

  public void start() throws IOException {
    statistic.taskStartTime = System.currentTimeMillis();
    // skip files without data
    loadTsFileNode.getResources().removeIf(f -> f.getDevices().isEmpty());
    uuid = UUID.randomUUID().toString();

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
    logger.info("Cleanup ends after {}ms", System.currentTimeMillis() - start);
    return success;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess) {
    logger.info("Start dispatching Load command for uuid {}", uuid);
    TLoadCommandReq loadCommandReq =
        new TLoadCommandReq(
            (isFirstPhaseSuccess ? LoadCommand.EXECUTE : LoadCommand.ROLLBACK).ordinal(), uuid);
    loadCommandReq.setIsGeneratedByPipe(isGeneratedByPipe);

    for (TRegionReplicaSet replicaSet : allReplicaSets) {
      loadCommandReq.setUseConsensus(true);
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
        Exception groupException = null;
        loadCommandReq.setConsensusGroupId(replicaSet.getRegionId());

        for (int i = 0; i < MAX_RETRY; i++) {
          try (SyncDataNodeInternalServiceClient client =
              internalServiceClientManager.borrowClient(endPoint)) {
            TLoadResp loadResp = client.sendLoadCommand(loadCommandReq);
            if (!loadResp.isAccepted()) {
              logger.warn(loadResp.message);
              groupException = new FragmentInstanceDispatchException(loadResp.status);
            }
            break;
          } catch (ClientManagerException | TException e) {
            logger.warn(NODE_CONNECTION_ERROR, endPoint, e);
            TSStatus status = new TSStatus();
            status.setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
            status.setMessage(
                "can't connect to node {}, please reset longer dn_connection_timeout_ms "
                    + "in iotdb-common.properties and restart iotdb."
                    + endPoint);
            groupException = new FragmentInstanceDispatchException(status);
          }
          try {
            Thread.sleep(RETRY_INTERVAL_MS);
          } catch (InterruptedException e) {
            groupException = e;
            break;
          }
        }

        if (groupException != null) {
          phaseTwoFailures.put(replicaSet, groupException);
        } else {
          break;
        }
      }
    }

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

  private boolean dispatchPieceNodes(
      List<LoadTsFilePieceNode> subNodes, TRegionReplicaSet replicaSet) {
    AtomicLong minDispatchTime = new AtomicLong(Long.MAX_VALUE);
    AtomicLong maxDispatchTime = new AtomicLong(Long.MIN_VALUE);
    AtomicLong sumDispatchTime = new AtomicLong();
    AtomicLong sumCompressingTime = new AtomicLong();

    long start = System.nanoTime();
    List<Boolean> subNodeResults =
        subNodes.stream()
            .parallel()
            .map(
                node -> {
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
                  sumCompressingTime.addAndGet(compressingTime);
                  statistic.compressingTime.addAndGet(compressingTime);

                  TTsFilePieceReq loadTsFileReq =
                      new TTsFilePieceReq(buffer, uuid, replicaSet.getRegionId());
                  loadTsFileReq.isRelay = true;
                  loadTsFileReq.setCompressionType(compressionType.serialize());
                  loadTsFileReq.setUncompressedLength(uncompressedLength);
                  LocationSequencer locationSequencer = createLocationSequencer(replicaSet);

                  boolean loadSucceed = false;
                  Exception lastConnectionError = null;
                  TDataNodeLocation currLocation = null;
                  for (TDataNodeLocation location : locationSequencer) {
                    if (location.getDataNodeId() == 0 && logger.isDebugEnabled()) {
                      locationStatistics.logLocationStatistics();
                      logger.info("Chose location {}", location.getDataNodeId());
                    }
                    currLocation = location;
                    startTime = System.nanoTime();
                    for (int i = 0; i < MAX_RETRY; i++) {
                      try (SyncDataNodeInternalServiceClient client =
                          internalServiceClientManager.borrowClient(
                              currLocation.internalEndPoint)) {
                        TLoadResp loadResp = client.sendTsFilePieceNode(loadTsFileReq);
                        logger.debug("Response from {}: {}", location.getDataNodeId(), loadResp);
                        if (!loadResp.isAccepted()) {
                          logger.warn(loadResp.message);
                          phaseOneFailures.put(
                              new Pair<>(node, replicaSet),
                              new FragmentInstanceDispatchException(loadResp.status));
                          return false;
                        }
                        loadSucceed = true;
                        break;
                      } catch (ClientManagerException | TException e) {
                        lastConnectionError = e;
                      }

                      try {
                        Thread.sleep(RETRY_INTERVAL_MS);
                      } catch (InterruptedException e) {
                        return false;
                      }
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
                        new Pair<>(node, replicaSet),
                        new FragmentInstanceDispatchException(status));
                    return false;
                  }
                  long timeConsumption = System.nanoTime() - startTime;
                  logger.debug("Time consumption: {}", timeConsumption);
                  locationStatistics.updateThroughput(
                      currLocation, node.getDataSize(), timeConsumption);

                  synchronized (maxDispatchTime) {
                    if (maxDispatchTime.get() < timeConsumption) {
                      maxDispatchTime.set(timeConsumption);
                    }
                  }
                  synchronized (minDispatchTime) {
                    if (minDispatchTime.get() > timeConsumption) {
                      minDispatchTime.set(timeConsumption);
                    }
                  }
                  sumDispatchTime.addAndGet(timeConsumption);
                  return true;
                })
            .collect(Collectors.toList());
    long elapsedTime = System.nanoTime() - start;
    statistic.dispatchNodesTime.addAndGet(elapsedTime);
    logger.debug(
        "Dispatched one node after {}ms, min/maxDispatching time: {}/{}ns, avg: {}ns, sum: {}ns, compressing: {}ns",
        elapsedTime / 1_000_000L,
        minDispatchTime.get(),
        maxDispatchTime.get(),
        sumDispatchTime.get() / subNodes.size(),
        sumDispatchTime.get(),
        sumCompressingTime.get());
    return !subNodeResults.contains(false);
  }

  public boolean dispatchOnePieceNode(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    long allStart = System.nanoTime();
    allReplicaSets.add(replicaSet);
    if (false) {
      long start = System.nanoTime();
      List<LoadTsFilePieceNode> split = pieceNodeSplitter.split(pieceNode);
      long elapsedTime = System.nanoTime() - start;
      statistic.splitTime.addAndGet(elapsedTime);
      statistic.pieceNodeNum.incrementAndGet();
      logger.debug("{} splits are generated after {}ms", split.size(), elapsedTime / 1_000_000L);
      boolean success = dispatchPieceNodes(split, replicaSet);
      statistic.dispatchNodeTime.addAndGet(System.nanoTime() - allStart);
      return success;
    }

    List<LoadTsFilePieceNode> subNodes;
    if (splitFutures.size() < MAX_PENDING_PIECE_NODE) {
      splitFutures.add(new Pair<>(submitSplitPieceNode(pieceNode), replicaSet));
      statistic.dispatchNodeTime.addAndGet(System.nanoTime() - allStart);
      return true;
    } else {
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
      boolean success = dispatchPieceNodes(subNodes, pair.right);
      statistic.dispatchNodeTime.addAndGet(System.nanoTime() - allStart);
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
    public AtomicLong dispatchNodesTime = new AtomicLong();
    public AtomicLong dispatchNodeTime = new AtomicLong();
    public AtomicLong compressingTime = new AtomicLong();

    public void logStatistic() {
      logger.info("Time consumption: {}ms", taskEndTime - taskStartTime);
      logger.info(
          "Generated {} piece nodes, splitTime: {}, dispatchSplitsTime: {}, dispatchNodeTime: {}",
          pieceNodeNum.get(),
          splitTime.get() / 1_000_000L,
          dispatchNodesTime.get() / 1_000_000L,
          dispatchNodeTime.get() / 1_000_000L);
      logger.info(
          "Transmission size: {}/{} ({}), compressionTime: {}ms",
          compressedSize.get(),
          rawSize.get(),
          compressedSize.get() * 1.0 / rawSize.get(),
          compressingTime.get() / 1_000_000L);
    }
  }

  public Statistic getStatistic() {
    return statistic;
  }
}
