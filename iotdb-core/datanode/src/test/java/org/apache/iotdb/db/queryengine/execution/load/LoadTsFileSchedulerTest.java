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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.junit.Test;

public class LoadTsFileSchedulerTest extends TestBase {

  protected Map<TEndPoint, Map<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>>>
      phaseOneResults = new ConcurrentSkipListMap<>();
  // the third key is UUid, the value is command type
  protected Map<TEndPoint, Map<String, Integer>> phaseTwoResults =
      new ConcurrentSkipListMap<>();

  @Test
  public void test() {
    MPPQueryContext context = new MPPQueryContext(QueryId.MOCK_QUERY_ID);

    PlanFragmentId fragmentId = new PlanFragmentId("load_tsfile_scheduler_test", 0);
    SubPlan subPlan = new SubPlan(
        new PlanFragment(fragmentId, null));
    List<FragmentInstance> fragmentInstanceList = new ArrayList<>();
    for (int i = 0; i < tsFileResources.size(); i++) {
      TsFileResource tsFileResource = tsFileResources.get(i);
      LoadSingleTsFileNode singleTsFileNode = new LoadSingleTsFileNode(
          new PlanNodeId("load_tsfile_scheduler_test" + (i + 1)), tsFileResource, false);
      fragmentInstanceList.add(new FragmentInstance(
          new PlanFragment(fragmentId, singleTsFileNode),
          new FragmentInstanceId(fragmentId, "" + i),
          null, null, 0, null));
    }
    DistributedQueryPlan queryPlan = new DistributedQueryPlan(context, subPlan, null,
        fragmentInstanceList);

    LoadTsFileScheduler scheduler = new LoadTsFileScheduler(queryPlan, context,
        new QueryStateMachine(context.getQueryId(),
            IoTDBThreadPoolFactory.newCachedThreadPool("LoadTsFileSchedulerTest")),
        internalServiceClientManager, dummyDataPartitionBatchFetcher(), false);

    long start = System.currentTimeMillis();
    scheduler.start();
    long timeConsumption = System.currentTimeMillis() - start;

    printPhaseResult();
    System.out.printf("Split ends after %dms", timeConsumption);
  }

  public TLoadResp handleTsFilePieceNode(TTsFilePieceReq req, TEndPoint tEndpoint) {
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

    return new TLoadResp().setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public TLoadResp handleTsLoadCommand(TLoadCommandReq req, TEndPoint tEndpoint) {
    phaseTwoResults
        .computeIfAbsent(tEndpoint,
            e -> new ConcurrentSkipListMap<>())
        .computeIfAbsent(req.uuid, id -> req.commandType);

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
        int chunkNum = 0;
        int fileNum = 0;
        int taskNum = 0;
        for (Entry<String, Map<File, Set<Integer>>> stringMapEntry :
            consensusGroupIdMapEntry.getValue().entrySet()) {;
          taskNum += 1;
          for (Entry<File, Set<Integer>> fileListEntry : stringMapEntry.getValue().entrySet()) {
            Set<Integer> chunks = fileListEntry.getValue();
            chunkNum += chunks.size();
            fileNum += 1;
          }
        }
        System.out.printf(
            "%s - %s - %s tasks - %s files - %s chunks\n", endPoint, consensusGroupId, taskNum, fileNum, chunkNum);
//        if (consensusGroupId.getId() == 0) {
//          // d1, non-aligned series
//          assertEquals(expectedChunkNum() / 2, chunkNum);
//        } else {
//          // d2, aligned series
//          assertEquals(expectedChunkNum() / 2 / seriesNum, chunkNum);
//        }
      }
    }

    System.out.print("Phase two:\n");
    for (Entry<TEndPoint, Map<String, Integer>> tEndPointMapEntry :
        phaseTwoResults.entrySet()) {
      TEndPoint endPoint = tEndPointMapEntry.getKey();
      for (Entry<String, Integer> stringMapEntry :
          tEndPointMapEntry.getValue().entrySet()) {
        String uuid = stringMapEntry.getKey();
        int command = stringMapEntry.getValue();
        System.out.printf("%s - %s - %s\n", endPoint, uuid,
            LoadCommand.values()[command]);
        assertEquals(LoadCommand.EXECUTE.ordinal(), command);
      }
    }
  }
}
