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

import static org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileDispatcherImpl.NODE_CONNECTION_ERROR;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.apache.iotdb.db.queryengine.execution.load.nodesplit.OrderedMeasurementSplitter;
import org.apache.iotdb.db.queryengine.execution.load.nodesplit.PieceNodeSplitter;
import org.apache.iotdb.db.queryengine.execution.load.nodesplit.PieceNodeSplitter.SingletonSplitter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileSplitSender {

  private static final int MAX_RETRY = 5;
  private static final long RETRY_INTERVAL_MS = 6_000L;
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
  private PieceNodeSplitter pieceNodeSplitter = new ClusteringMeasurementSplitter(10, 10);
//  private PieceNodeSplitter pieceNodeSplitter = new OrderedMeasurementSplitter();

  public TsFileSplitSender(
      LoadTsFileNode loadTsFileNode,
      DataPartitionBatchFetcher targetPartitionFetcher,
      long targetPartitionInterval,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      boolean isGeneratedByPipe,
      long maxSplitSize) {
    this.loadTsFileNode = loadTsFileNode;
    this.targetPartitionFetcher = targetPartitionFetcher;
    this.targetPartitionInterval = targetPartitionInterval;
    this.internalServiceClientManager = internalServiceClientManager;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.maxSplitSize = maxSplitSize;
  }

  public void start() throws IOException {
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
    locationStatistics.logLocationStatistics();
  }

  private boolean firstPhase(LoadTsFileNode node) throws IOException {
    TsFileDataManager tsFileDataManager =
        new DeviceBatchTsFileDataManager(
            this::dispatchOnePieceNode,
            node.getPlanNodeId(),
            node.lastResource().getTsFile(),
            targetPartitionFetcher,
            maxSplitSize);

    ExecutorService executorService =
        IoTDBThreadPoolFactory.newThreadPool(
            32,
            Integer.MAX_VALUE,
            20,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new IoTThreadFactory("MergedTsFileSplitter"),
            "MergedTsFileSplitter");
    MergedTsFileSplitter splitter = new MergedTsFileSplitter(
        node.getResources().stream()
            .map(TsFileResource::getTsFile)
            .collect(Collectors.toList()),
        tsFileDataManager::addOrSendTsFileData,
        executorService,
        targetPartitionInterval);
    splitter.splitTsFileByDataPartition();
    splitter.close();
    return tsFileDataManager.sendAllTsFileData() && phaseOneFailures.isEmpty();
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

  public boolean dispatchOnePieceNode(LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    allReplicaSets.add(replicaSet);

    long start = System.currentTimeMillis();
    List<LoadTsFilePieceNode> subNodes = pieceNodeSplitter.split(pieceNode);
    logger.info("{} splits are generated after {}ms", subNodes.size(), System.currentTimeMillis() - start);

    List<Boolean> subNodeResults = subNodes.stream().parallel().map(node -> {
      long startTime = 0;
      TTsFilePieceReq loadTsFileReq =
          new TTsFilePieceReq(node.serializeToByteBuffer(), uuid, replicaSet.getRegionId());
      loadTsFileReq.isRelay = true;
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
              internalServiceClientManager.borrowClient(currLocation.internalEndPoint)) {
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
            new Pair<>(node, replicaSet), new FragmentInstanceDispatchException(status));
        return false;
      }
      long timeConsumption = System.nanoTime() - startTime;
      logger.debug("Time consumption: {}", timeConsumption);
      locationStatistics.updateThroughput(currLocation, node.getDataSize(), timeConsumption);
      return true;
    }).collect(Collectors.toList());

    return !subNodeResults.contains(false);
  }
}
