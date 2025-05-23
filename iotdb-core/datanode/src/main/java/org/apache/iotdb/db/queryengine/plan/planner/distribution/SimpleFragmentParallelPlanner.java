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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.ClusterTopology;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.AbstractFragmentParallelPlanner;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple implementation of IFragmentParallelPlaner. This planner will transform one PlanFragment
 * into only one FragmentInstance.
 */
public class SimpleFragmentParallelPlanner extends AbstractFragmentParallelPlanner {

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
  private final ClusterTopology topology = ClusterTopology.getInstance();

  public SimpleFragmentParallelPlanner(
      SubPlan subPlan, Analysis analysis, MPPQueryContext context) {
    super(context);
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
            queryContext.getTimeOut() - (System.currentTimeMillis() - queryContext.getStartTime()),
            queryContext.getSession(),
            queryContext.isExplainAnalyze(),
            fragment.isRoot());

    selectExecutorAndHost(
        fragment,
        fragmentInstance,
        fragment::getTargetRegionForTreeModel,
        topology::getValidatedReplicaSet,
        dataNodeFIMap);

    if (analysis.getTreeStatement() instanceof QueryStatement
        || analysis.getTreeStatement() instanceof ExplainAnalyzeStatement
        || analysis.getTreeStatement() instanceof ShowQueriesStatement
        || (analysis.getTreeStatement() instanceof ShowTimeSeriesStatement
            && ((ShowTimeSeriesStatement) analysis.getTreeStatement()).isOrderByHeat())) {
      fragmentInstance.getFragment().generateTypeProvider(queryContext.getTypeProvider());
    }
    instanceMap.putIfAbsent(fragment.getId(), fragmentInstance);
    fragment.setIndexInFragmentInstanceList(fragmentInstanceList.size());
    fragmentInstanceList.add(fragmentInstance);
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
                  FragmentInstance downStreamInstance =
                      findDownStreamInstance(planNodeMap, instanceMap, downStreamNodeId);
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

  private void recordPlanNodeRelation(PlanNode root, PlanFragmentId planFragmentId) {
    planNodeMap.put(root.getPlanNodeId(), new Pair<>(planFragmentId, root));
    for (PlanNode child : root.getChildren()) {
      recordPlanNodeRelation(child, planFragmentId);
    }
  }
}
