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

package org.apache.iotdb.db.queryengine.load.scheduler;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class LoadTsFileSchedulerNew implements IScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileSchedulerNew.class);

  private static final int TRANSMIT_LIMIT =
      CommonDescriptor.getInstance().getConfig().getTTimePartitionSlotTransmitLimit();

  private final MPPQueryContext queryContext;
  private final QueryStateMachine stateMachine;
  private final DataPartitionBatchFetcher partitionFetcher;
  private final List<LoadSingleTsFileStateMachine> tsFileStateMachineList;
  private final PlanFragmentId fragmentId;
  private final boolean isGeneratedByPipe;
  private final LoadTsFileSplitterManager splitterManager;

  private CountDownLatch tsFileLoadCompletionCount;

  public LoadTsFileSchedulerNew(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IPartitionFetcher partitionFetcher,
      boolean isGeneratedByPipe,
      LoadTsFileSplitterManager splitterManager) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.partitionFetcher = new DataPartitionBatchFetcher(partitionFetcher);
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.splitterManager = splitterManager;

    this.tsFileStateMachineList =
        ImmutableList.<LoadSingleTsFileStateMachine>builderWithExpectedSize(
                distributedQueryPlan.getInstances().size())
            .addAll(
                distributedQueryPlan.getInstances().stream()
                    .map(
                        fragmentInstance ->
                            new LoadSingleTsFileStateMachine(
                                this,
                                (LoadSingleTsFileNode)
                                    fragmentInstance.getFragment().getPlanNodeTree()))
                    .collect(Collectors.toList()))
            .build();
  }

  @Override
  public void start() {
    if (tsFileLoadCompletionCount == null) {
      LOGGER.info("Start to load {} TsFiles", tsFileStateMachineList.size());
    } else {
      LOGGER.warn(
          "Abort load {} TsFiles, reload {} TsFiles",
          tsFileLoadCompletionCount.getCount(),
          tsFileStateMachineList.size());
    }
    tsFileLoadCompletionCount = new CountDownLatch(tsFileStateMachineList.size());

    splitterManager.submitTsFileStateMachines(tsFileStateMachineList);
    try {
      tsFileLoadCompletionCount.await();
    } catch (InterruptedException e) {
      LOGGER.error("Load TsFile interrupted", e);
    }
  }

  @Override
  public void stop(Throwable t) {}

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  void tsFileLoadCompleteCountDown() {
    tsFileLoadCompletionCount.countDown();
    LOGGER.info(
        "Finish loading [{}/{}] TsFiles",
        tsFileStateMachineList.size() - tsFileLoadCompletionCount.getCount(),
        tsFileStateMachineList.size());
  }

  private static class DataPartitionBatchFetcher {
    private final IPartitionFetcher fetcher;

    public DataPartitionBatchFetcher(IPartitionFetcher fetcher) {
      this.fetcher = fetcher;
    }

    public List<TRegionReplicaSet> queryDataPartition(
        List<Pair<String, TTimePartitionSlot>> slotList, String userName) {
      List<TRegionReplicaSet> replicaSets = new ArrayList<>();
      int size = slotList.size();

      for (int i = 0; i < size; i += TRANSMIT_LIMIT) {
        List<Pair<String, TTimePartitionSlot>> subSlotList =
            slotList.subList(i, Math.min(size, i + TRANSMIT_LIMIT));
        DataPartition dataPartition =
            fetcher.getOrCreateDataPartition(toQueryParam(subSlotList), userName);
        replicaSets.addAll(
            subSlotList.stream()
                .map(pair -> dataPartition.getDataRegionReplicaSetForWriting(pair.left, pair.right))
                .collect(Collectors.toList()));
      }
      return replicaSets;
    }

    private List<DataPartitionQueryParam> toQueryParam(
        List<Pair<String, TTimePartitionSlot>> slots) {
      return slots.stream()
          .collect(
              Collectors.groupingBy(
                  Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toSet())))
          .entrySet()
          .stream()
          .map(
              entry ->
                  new DataPartitionQueryParam(entry.getKey(), new ArrayList<>(entry.getValue())))
          .collect(Collectors.toList());
    }
  }
}
