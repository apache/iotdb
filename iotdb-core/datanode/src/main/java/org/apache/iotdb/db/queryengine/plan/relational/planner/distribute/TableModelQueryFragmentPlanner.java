/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.ClusterTopology;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.AbstractFragmentParallelPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableModelQueryFragmentPlanner extends AbstractFragmentParallelPlanner {

  private final SubPlan subPlan;

  private final Analysis analysis;

  private final List<FragmentInstance> fragmentInstanceList = new ArrayList<>();

  private final MPPQueryContext queryContext;

  // Record all the FragmentInstances belonged to same PlanFragment
  private final Map<PlanFragmentId, FragmentInstance> instanceMap = new HashMap<>();

  // Record which PlanFragment the PlanNode belongs
  private final Map<PlanNodeId, Pair<PlanFragmentId, PlanNode>> planNodeMap = new HashMap<>();

  // Record FragmentInstances dispatched to same DataNode
  private final Map<TDataNodeLocation, List<FragmentInstance>> dataNodeFIMap = new HashMap<>();
  private final ClusterTopology topology = ClusterTopology.getInstance();

  private final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;

  TableModelQueryFragmentPlanner(
      SubPlan subPlan,
      Analysis analysis,
      MPPQueryContext queryContext,
      final Map<PlanNodeId, NodeDistribution> nodeDistributionMap) {
    super(queryContext);
    this.subPlan = subPlan;
    this.analysis = analysis;
    this.queryContext = queryContext;
    this.nodeDistributionMap = nodeDistributionMap;
  }

  @Override
  public List<FragmentInstance> parallelPlan() {
    prepare();
    calculateNodeTopologyBetweenInstance();
    return fragmentInstanceList;
  }

  private void prepare() {
    for (PlanFragment fragment : subPlan.getPlanFragmentList()) {
      recordPlanNodeRelation(fragment.getPlanNodeTree(), fragment.getId());
      produceFragmentInstance(fragment, nodeDistributionMap);
    }

    fragmentInstanceList.forEach(
        fi -> fi.setDataNodeFINum(dataNodeFIMap.get(fi.getHostDataNode()).size()));
  }

  private void recordPlanNodeRelation(PlanNode root, PlanFragmentId planFragmentId) {
    planNodeMap.put(root.getPlanNodeId(), new Pair<>(planFragmentId, root));
    root.getChildren().forEach(child -> recordPlanNodeRelation(child, planFragmentId));
  }

  private void calculateNodeTopologyBetweenInstance() {
    for (FragmentInstance instance : fragmentInstanceList) {
      PlanNode rootNode = instance.getFragment().getPlanNodeTree();
      if (rootNode instanceof MultiChildrenSinkNode) {
        MultiChildrenSinkNode sinkNode = (MultiChildrenSinkNode) rootNode;
        for (DownStreamChannelLocation downStreamChannelLocation :
            sinkNode.getDownStreamChannelLocationList()) {
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
        }
      }
    }
  }

  private void produceFragmentInstance(
      PlanFragment fragment, final Map<PlanNodeId, NodeDistribution> nodeDistributionMap) {
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            fragment,
            fragment.getId().genFragmentInstanceId(),
            QueryType.READ,
            queryContext.getTimeOut() - (System.currentTimeMillis() - queryContext.getStartTime()),
            queryContext.getSession(),
            queryContext.isExplainAnalyze(),
            fragment.isRoot());

    selectExecutorAndHost(
        fragment,
        fragmentInstance,
        () -> fragment.getTargetRegionForTableModel(nodeDistributionMap),
        topology::getValidatedReplicaSet,
        dataNodeFIMap);

    final Statement statement = analysis.getStatement();
    if (analysis.isQuery() || statement instanceof ShowDevice || statement instanceof CountDevice) {
      fragmentInstance.getFragment().generateTableModelTypeProvider(queryContext.getTypeProvider());
    }
    instanceMap.putIfAbsent(fragment.getId(), fragmentInstance);
    fragment.setIndexInFragmentInstanceList(fragmentInstanceList.size());
    fragmentInstanceList.add(fragmentInstance);
  }
}
