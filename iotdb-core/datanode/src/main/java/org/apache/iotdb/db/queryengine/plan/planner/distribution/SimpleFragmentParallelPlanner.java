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
package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.TreeModelTimePredicate;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastSeriesSourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;

import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple implementation of IFragmentParallelPlaner. This planner will transform one PlanFragment
 * into only one FragmentInstance.
 */
public class SimpleFragmentParallelPlanner implements IFragmentParallelPlaner {
  private static final Logger logger = LoggerFactory.getLogger(SimpleFragmentParallelPlanner.class);

  private final SubPlan subPlan;
  private final Analysis analysis;
  private final MPPQueryContext queryContext;

  // Record all the FragmentInstances belonged to same PlanFragment
  private final Map<PlanFragmentId, FragmentInstance> instanceMap;
  // Record which PlanFragment the PlanNode belongs
  private final Map<PlanNodeId, Pair<PlanFragmentId, PlanNode>> planNodeMap;
  private final List<FragmentInstance> fragmentInstanceList;

  // Record FragmentInstances dispatched to same DataNode
  private final Map<TDataNodeLocation, List<FragmentInstance>> dataNodeFIMap;

  public SimpleFragmentParallelPlanner(
      SubPlan subPlan, Analysis analysis, MPPQueryContext context) {
    this.subPlan = subPlan;
    this.analysis = analysis;
    this.queryContext = context;
    this.instanceMap = new HashMap<>();
    this.planNodeMap = new HashMap<>();
    this.fragmentInstanceList = new ArrayList<>();
    this.dataNodeFIMap = new HashMap<>();
  }

  @Override
  public List<FragmentInstance> parallelPlan() {
    prepare();
    calculateNodeTopologyBetweenInstance();
    return fragmentInstanceList;
  }

  private void prepare() {
    List<PlanFragment> fragments = subPlan.getPlanFragmentList();
    for (PlanFragment fragment : fragments) {
      recordPlanNodeRelation(fragment.getPlanNodeTree(), fragment.getId());
      produceFragmentInstance(fragment);
    }
    fragmentInstanceList.forEach(
        fragmentInstance ->
            fragmentInstance.setDataNodeFINum(
                dataNodeFIMap.get(fragmentInstance.getHostDataNode()).size()));

    // compute dataNodeSeriesScanNum in LastQueryScanNode
    if (analysis.getTreeStatement() instanceof QueryStatement
        && ((QueryStatement) analysis.getTreeStatement()).isLastQuery()) {
      final Map<Path, AtomicInteger> pathSumMap = new HashMap<>();
      dataNodeFIMap
          .values()
          .forEach(
              fragmentInstances -> {
                fragmentInstances.forEach(
                    fragmentInstance ->
                        updateScanNum(
                            fragmentInstance.getFragment().getPlanNodeTree(), pathSumMap));
                pathSumMap.clear();
              });
    }
  }

  private void updateScanNum(PlanNode planNode, Map<Path, AtomicInteger> pathSumMap) {
    if (planNode instanceof LastSeriesSourceNode) {
      LastSeriesSourceNode lastSeriesSourceNode = (LastSeriesSourceNode) planNode;
      pathSumMap.merge(
          lastSeriesSourceNode.getSeriesPath(),
          lastSeriesSourceNode.getDataNodeSeriesScanNum(),
          (k, v) -> {
            v.incrementAndGet();
            return v;
          });
    }
    planNode.getChildren().forEach(node -> updateScanNum(node, pathSumMap));
  }

  private void produceFragmentInstance(PlanFragment fragment) {
    Expression globalTimePredicate = analysis.getGlobalTimePredicate();
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            fragment,
            fragment.getId().genFragmentInstanceId(),
            globalTimePredicate == null ? null : new TreeModelTimePredicate(globalTimePredicate),
            queryContext.getQueryType(),
            queryContext.getTimeOut(),
            queryContext.getSession(),
            queryContext.isExplainAnalyze(),
            fragment.isRoot());

    // Get the target region for origin PlanFragment, then its instance will be distributed one
    // of them.
    TRegionReplicaSet regionReplicaSet = fragment.getTargetRegion();

    // Set ExecutorType and target host for the instance
    // We need to store all the replica host in case of the scenario that the instance need to be
    // redirected
    // to another host when scheduling
    if (regionReplicaSet == null || regionReplicaSet.getRegionId() == null) {
      TDataNodeLocation dataNodeLocation = fragment.getTargetLocation();
      if (dataNodeLocation != null) {
        // now only the case ShowQueries will enter here
        fragmentInstance.setExecutorAndHost(new QueryExecutor(dataNodeLocation));
      } else {
        // no data region && no dataNodeLocation, we need to execute this FI on local
        // now only the case AggregationQuery has schemaengine but no data region will enter here
        fragmentInstance.setExecutorAndHost(
            new QueryExecutor(DataNodeEndPoints.getLocalDataNodeLocation()));
      }
    } else {
      fragmentInstance.setExecutorAndHost(new StorageExecutor(regionReplicaSet));
      fragmentInstance.setHostDataNode(selectTargetDataNode(regionReplicaSet));
    }

    dataNodeFIMap.compute(
        fragmentInstance.getHostDataNode(),
        (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(fragmentInstance);
          return v;
        });

    if (analysis.getTreeStatement() instanceof QueryStatement
        || analysis.getTreeStatement() instanceof ExplainAnalyzeStatement
        || analysis.getTreeStatement() instanceof ShowQueriesStatement
        || (analysis.getTreeStatement() instanceof ShowTimeSeriesStatement
            && ((ShowTimeSeriesStatement) analysis.getTreeStatement()).isOrderByHeat())) {
      fragmentInstance.getFragment().generateTypeProvider(queryContext.getTypeProvider());
    }
    instanceMap.putIfAbsent(fragment.getId(), fragmentInstance);
    fragmentInstanceList.add(fragmentInstance);
  }

  private TDataNodeLocation selectTargetDataNode(TRegionReplicaSet regionReplicaSet) {
    if (regionReplicaSet == null
        || regionReplicaSet.getDataNodeLocations() == null
        || regionReplicaSet.getDataNodeLocations().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("regionReplicaSet is invalid: %s", regionReplicaSet));
    }
    String readConsistencyLevel =
        IoTDBDescriptor.getInstance().getConfig().getReadConsistencyLevel();
    // TODO: (Chen Rongzhao) need to make the values of ReadConsistencyLevel as static variable or
    // enums
    boolean selectRandomDataNode = "weak".equals(readConsistencyLevel);

    // When planning fragment onto specific DataNode, the DataNode whose endPoint is in
    // black list won't be considered because it may have connection issue now.
    List<TDataNodeLocation> availableDataNodes =
        filterAvailableTDataNode(regionReplicaSet.getDataNodeLocations());
    if (availableDataNodes.isEmpty()) {
      String errorMsg =
          String.format(
              "All replicas for region[%s] are not available in these DataNodes[%s]",
              regionReplicaSet.getRegionId(), regionReplicaSet.getDataNodeLocations());
      throw new IllegalArgumentException(errorMsg);
    }
    if (regionReplicaSet.getDataNodeLocationsSize() != availableDataNodes.size()) {
      logger.info("available replicas: {}", availableDataNodes);
    }
    int targetIndex;
    if (!selectRandomDataNode || queryContext.getSession() == null) {
      targetIndex = 0;
    } else {
      targetIndex = (int) (queryContext.getSession().getSessionId() % availableDataNodes.size());
    }
    return availableDataNodes.get(targetIndex);
  }

  private List<TDataNodeLocation> filterAvailableTDataNode(
      List<TDataNodeLocation> originalDataNodeList) {
    List<TDataNodeLocation> result = new LinkedList<>();
    for (TDataNodeLocation dataNodeLocation : originalDataNodeList) {
      if (isAvailableDataNode(dataNodeLocation)) {
        result.add(dataNodeLocation);
      }
    }
    return result;
  }

  private boolean isAvailableDataNode(TDataNodeLocation dataNodeLocation) {
    for (TEndPoint endPoint : queryContext.getEndPointBlackList()) {
      if (endPoint.equals(dataNodeLocation.internalEndPoint)) {
        return false;
      }
    }
    return true;
  }

  private void calculateNodeTopologyBetweenInstance() {
    for (FragmentInstance instance : fragmentInstanceList) {
      PlanNode rootNode = instance.getFragment().getPlanNodeTree();
      if (rootNode instanceof MultiChildrenSinkNode) {
        MultiChildrenSinkNode sinkNode = (MultiChildrenSinkNode) rootNode;
        sinkNode
            .getDownStreamChannelLocationList()
            .forEach(
                downStreamChannelLocation -> {
                  // Set target Endpoint for FragmentSinkNode
                  PlanNodeId downStreamNodeId =
                      new PlanNodeId(downStreamChannelLocation.getRemotePlanNodeId());
                  FragmentInstance downStreamInstance = findDownStreamInstance(downStreamNodeId);
                  downStreamChannelLocation.setRemoteEndpoint(
                      downStreamInstance.getHostDataNode().getMPPDataExchangeEndPoint());
                  downStreamChannelLocation.setRemoteFragmentInstanceId(
                      downStreamInstance.getId().toThrift());

                  // Set upstream info for corresponding ExchangeNode in downstream FragmentInstance
                  PlanNode downStreamExchangeNode = planNodeMap.get(downStreamNodeId).right;
                  ((ExchangeNode) downStreamExchangeNode)
                      .setUpstream(
                          instance.getHostDataNode().getMPPDataExchangeEndPoint(),
                          instance.getId(),
                          sinkNode.getPlanNodeId());
                });
      }
    }
  }

  private FragmentInstance findDownStreamInstance(PlanNodeId exchangeNodeId) {
    return instanceMap.get(planNodeMap.get(exchangeNodeId).left);
  }

  private void recordPlanNodeRelation(PlanNode root, PlanFragmentId planFragmentId) {
    planNodeMap.put(root.getPlanNodeId(), new Pair<>(planFragmentId, root));
    for (PlanNode child : root.getChildren()) {
      recordPlanNodeRelation(child, planFragmentId);
    }
  }
}
