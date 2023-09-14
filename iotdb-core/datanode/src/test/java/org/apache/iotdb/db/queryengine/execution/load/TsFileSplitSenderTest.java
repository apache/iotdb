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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.thrift.TException;
import org.junit.Test;

public class TsFileSplitSenderTest extends TestBase {

  protected Map<TEndPoint, Map<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>>>
      phaseOneResults = new ConcurrentSkipListMap<>();
  // the third key is UUid, the value is command type
  protected Map<TEndPoint, Map<ConsensusGroupId, Map<String, Integer>>> phaseTwoResults =
      new ConcurrentSkipListMap<>();
  private long dummyDelayMS = 200;
  private double packetLossRatio = 0.02;
  private Random random = new Random();
  private long maxSplitSize = 128*1024*1024;


  @Test
  public void test() throws IOException {
    LoadTsFileNode loadTsFileNode =
        new LoadTsFileNode(new PlanNodeId("testPlanNode"), tsFileResources);
    DataPartitionBatchFetcher partitionBatchFetcher =
        dummyDataPartitionBatchFetcher();
    TsFileSplitSender splitSender =
        new TsFileSplitSender(
            loadTsFileNode,
            partitionBatchFetcher,
            TimePartitionUtils.getTimePartitionInterval(),
            internalServiceClientManager,
            false,
            maxSplitSize);
    long start = System.currentTimeMillis();
    splitSender.start();
    long timeConsumption = System.currentTimeMillis() - start;

    printPhaseResult();
    System.out.printf("Split ends after %dms", timeConsumption);
  }

  public TLoadResp handleTsFilePieceNode(TTsFilePieceReq req, TEndPoint tEndpoint)
      throws TException {
    if ((tEndpoint.getPort() - 10000) % 3 == 0 && random.nextDouble() < packetLossRatio && req.isRelay) {
      throw new TException("Packet lost");
    }
    if ((tEndpoint.getPort() - 10000) % 3 == 1 && random.nextDouble() < packetLossRatio / 2 && req.isRelay) {
      throw new TException("Packet lost");
    }

    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    LoadTsFilePieceNode pieceNode = (LoadTsFilePieceNode) PlanNodeType.deserialize(
        req.body.slice());
    Set<Integer> splitIds =
        phaseOneResults
            .computeIfAbsent(tEndpoint, e -> new ConcurrentSkipListMap<>(
                Comparator.comparingInt(ConsensusGroupId::getId)))
            .computeIfAbsent(groupId, g -> new ConcurrentSkipListMap<>())
            .computeIfAbsent(req.uuid, id -> new ConcurrentSkipListMap<>())
            .computeIfAbsent(pieceNode.getTsFile(), f -> new ConcurrentSkipListSet<>());
    splitIds.addAll(pieceNode.getAllTsFileData().stream().map(TsFileData::getSplitId).collect(
        Collectors.toList()));

    if (dummyDelayMS > 0) {
      if ((tEndpoint.getPort() - 10000) % 3 == 0 && req.isRelay) {
        try {
          Thread.sleep(dummyDelayMS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      if ((tEndpoint.getPort() - 10000) % 3 == 1 && req.isRelay) {
        try {
          Thread.sleep(dummyDelayMS / 2);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    // forward to other replicas in the group
    if (req.isRelay) {
      req.isRelay = false;
      TRegionReplicaSet regionReplicaSet = groupId2ReplicaSetMap.get(groupId);
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint otherPoint = dataNodeLocation.getInternalEndPoint();
        if (!otherPoint.equals(tEndpoint)) {
          handleTsFilePieceNode(req, otherPoint);
        }
      }
    }

    return new TLoadResp().setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public TLoadResp handleTsLoadCommand(TLoadCommandReq req, TEndPoint tEndpoint) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    phaseTwoResults
        .computeIfAbsent(tEndpoint,
            e -> new ConcurrentSkipListMap<>(Comparator.comparingInt(ConsensusGroupId::getId)))
        .computeIfAbsent(groupId, g -> new ConcurrentSkipListMap<>())
        .computeIfAbsent(req.uuid, id -> req.commandType);

    // forward to other replicas in the group
    if (req.useConsensus) {
      req.useConsensus = false;
      TRegionReplicaSet regionReplicaSet = groupId2ReplicaSetMap.get(groupId);
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint otherPoint = dataNodeLocation.getInternalEndPoint();
        if (!otherPoint.equals(tEndpoint)) {
          handleTsLoadCommand(req, otherPoint);
        }
      }
    }

    return new TLoadResp().setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public void printPhaseResult() {
    System.out.print("Phase one:\n");
    for (Entry<TEndPoint, Map<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>>>
        tEndPointMapEntry : phaseOneResults.entrySet()) {
      TEndPoint endPoint = tEndPointMapEntry.getKey();
      for (Entry<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>>
          consensusGroupIdMapEntry : tEndPointMapEntry.getValue().entrySet()) {
        ConsensusGroupId consensusGroupId = consensusGroupIdMapEntry.getKey();
        for (Entry<String, Map<File, Set<Integer>>> stringMapEntry :
            consensusGroupIdMapEntry.getValue().entrySet()) {
          String uuid = stringMapEntry.getKey();
          for (Entry<File, Set<Integer>> fileListEntry : stringMapEntry.getValue().entrySet()) {
            File tsFile = fileListEntry.getKey();
            Set<Integer> chunks = fileListEntry.getValue();
            System.out.printf(
                "%s - %s - %s - %s - %s chunks\n", endPoint, consensusGroupId, uuid, tsFile, chunks.size());
//            if (consensusGroupId.getId() == 0) {
//              // d1, non-aligned series
//              assertEquals(expectedChunkNum() / 2, chunks.size());
//            } else {
//              // d2, aligned series
//              assertEquals(expectedChunkNum() / 2 / seriesNum, chunks.size());
//            }
          }
        }
      }
    }

    System.out.print("Phase two:\n");
    for (Entry<TEndPoint, Map<ConsensusGroupId, Map<String, Integer>>> tEndPointMapEntry :
        phaseTwoResults.entrySet()) {
      TEndPoint endPoint = tEndPointMapEntry.getKey();
      for (Entry<ConsensusGroupId, Map<String, Integer>> consensusGroupIdMapEntry :
          tEndPointMapEntry.getValue().entrySet()) {
        ConsensusGroupId consensusGroupId = consensusGroupIdMapEntry.getKey();
        for (Entry<String, Integer> stringMapEntry :
            consensusGroupIdMapEntry.getValue().entrySet()) {
          String uuid = stringMapEntry.getKey();
          int command = stringMapEntry.getValue();
          System.out.printf("%s - %s - %s - %s\n", endPoint, consensusGroupId, uuid,
              LoadCommand.values()[command]);
          assertEquals(LoadCommand.EXECUTE.ordinal(), command);
        }
      }
    }
  }
}
