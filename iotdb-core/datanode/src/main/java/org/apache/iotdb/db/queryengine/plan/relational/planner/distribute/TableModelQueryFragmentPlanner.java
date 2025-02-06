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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TableModelQueryFragmentPlanner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableModelQueryFragmentPlanner.class);

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

  TableModelQueryFragmentPlanner(SubPlan subPlan, Analysis analysis, MPPQueryContext queryContext) {
    this.subPlan = subPlan;
    this.analysis = analysis;
    this.queryContext = queryContext;
  }

  public List<FragmentInstance> plan() {
    prepare();
    calculateNodeTopologyBetweenInstance();
    return fragmentInstanceList;
  }

  private void prepare() {
    for (PlanFragment fragment : subPlan.getPlanFragmentList()) {
      recordPlanNodeRelation(fragment.getPlanNodeTree(), fragment.getId());
      produceFragmentInstance(fragment);
    }

    fragmentInstanceList.forEach(
        fi -> fi.setDataNodeFINum(dataNodeFIMap.get(fi.getHostDataNode()).size()));
  }

  private void recordPlanNodeRelation(PlanNode root, PlanFragmentId planFragmentId) {
    planNodeMap.put(root.getPlanNodeId(), new Pair<>(planFragmentId, root));
    root.getChildren().forEach(child -> recordPlanNodeRelation(child, planFragmentId));
  }

  private void produceFragmentInstance(PlanFragment fragment) {
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            fragment,
            fragment.getId().genFragmentInstanceId(),
            QueryType.READ,
            queryContext.getTimeOut(),
            queryContext.getSession(),
            queryContext.isExplainAnalyze(),
            fragment.isRoot());

    // Get the target region for origin PlanFragment, then its instance will be distributed one
    // of them.
    TRegionReplicaSet regionReplicaSet = fragment.getTargetRegion();

    // Set ExecutorType and target host for the instance,
    // We need to store all the replica host in case of the scenario that the instance need to be
    // redirected
    // to another host when scheduling
    if (regionReplicaSet == null || regionReplicaSet.getRegionId() == null) {
      TDataNodeLocation dataNodeLocation = fragment.getTargetLocation();
      if (dataNodeLocation != null) {
        // now only the case ShowStatement will enter here
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

    final Statement statement = analysis.getStatement();
    if (analysis.isQuery() || statement instanceof ShowDevice || statement instanceof CountDevice) {
      fragmentInstance.getFragment().generateTableModelTypeProvider(queryContext.getTypeProvider());
    }
    instanceMap.putIfAbsent(fragment.getId(), fragmentInstance);
    fragmentInstanceList.add(fragmentInstance);
  }

  private TDataNodeLocation selectTargetDataNode(TRegionReplicaSet regionReplicaSet) {
    if (regionReplicaSet == null
        || regionReplicaSet.getDataNodeLocations() == null
        || regionReplicaSet.getDataNodeLocations().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("RegionReplicaSet is invalid: %s", regionReplicaSet));
    }
    String readConsistencyLevel =
        IoTDBDescriptor.getInstance().getConfig().getReadConsistencyLevel();
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
      LOGGER.info("Available replicas: {}", availableDataNodes);
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
        for (DownStreamChannelLocation downStreamChannelLocation :
            sinkNode.getDownStreamChannelLocationList()) {
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
        }
      }
    }
  }

  private FragmentInstance findDownStreamInstance(PlanNodeId exchangeNodeId) {
    return instanceMap.get(planNodeMap.get(exchangeNodeId).left);
  }
}
