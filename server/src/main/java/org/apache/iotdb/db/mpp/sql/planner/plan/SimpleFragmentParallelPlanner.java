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
package org.apache.iotdb.db.mpp.sql.planner.plan;

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple implementation of IFragmentParallelPlaner. This planner will transform one PlanFragment
 * into only one FragmentInstance.
 */
public class SimpleFragmentParallelPlanner implements IFragmentParallelPlaner {

  private SubPlan subPlan;

  // Record all the FragmentInstances belonged to same PlanFragment
  Map<PlanFragmentId, FragmentInstance> instanceMap;
  // Record which PlanFragment the PlanNode belongs
  Map<PlanNodeId, PlanFragmentId> planNodeMap;
  List<FragmentInstance> fragmentInstanceList;

  public SimpleFragmentParallelPlanner(SubPlan subPlan) {
    this.subPlan = subPlan;
    this.instanceMap = new HashMap<>();
    this.planNodeMap = new HashMap<>();
    this.fragmentInstanceList = new ArrayList<>();
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
      recordPlanNodeRelation(fragment.getRoot(), fragment.getId());
      produceFragmentInstance(fragment);
    }
  }

  private void produceFragmentInstance(PlanFragment fragment) {
    // If one PlanFragment will produce several FragmentInstance, the instanceIdx will be increased
    // one by one
    int instanceIdx = 0;
    PlanNode rootCopy = PlanNodeUtil.deepCopy(fragment.getRoot());
    FragmentInstance fragmentInstance =
        new FragmentInstance(new PlanFragment(fragment.getId(), rootCopy), instanceIdx);

    // Get the target DataRegion for origin PlanFragment, then its instance will be distributed one
    // of them.
    RegionReplicaSet dataRegion = fragment.getTargetDataRegion();

    // Set DataRegion and target host for the instance
    // We need to store all the replica host in case of the scenario that the instance need to be
    // redirected
    // to another host when scheduling
    fragmentInstance.setDataRegionId(dataRegion);

    // TODO: (xingtanzjr) We select the first Endpoint as the default target host for current
    // instance
    fragmentInstance.setHostEndpoint(dataRegion.getDataNodeList().get(0).getEndPoint());
    instanceMap.putIfAbsent(fragment.getId(), fragmentInstance);
    fragmentInstanceList.add(fragmentInstance);
  }

  private void calculateNodeTopologyBetweenInstance() {
    for (FragmentInstance instance : fragmentInstanceList) {
      PlanNode rootNode = instance.getFragment().getRoot();
      if (rootNode instanceof FragmentSinkNode) {
        // Set target Endpoint for FragmentSinkNode
        FragmentSinkNode sinkNode = (FragmentSinkNode) rootNode;
        PlanNodeId downStreamNodeId = sinkNode.getDownStreamPlanNodeId();
        FragmentInstance downStreamInstance = findDownStreamInstance(downStreamNodeId);
        sinkNode.setDownStream(
            downStreamInstance.getHostEndpoint(), downStreamInstance.getId(), downStreamNodeId);

        // Set upstream info for corresponding ExchangeNode in downstream FragmentInstance
        PlanNode downStreamExchangeNode =
            downStreamInstance.getFragment().getPlanNodeById(downStreamNodeId);
        ((ExchangeNode) downStreamExchangeNode)
            .setUpstream(instance.getHostEndpoint(), instance.getId(), sinkNode.getPlanNodeId());
      }
    }
  }

  private FragmentInstance findDownStreamInstance(PlanNodeId exchangeNodeId) {
    return instanceMap.get(planNodeMap.get(exchangeNodeId));
  }

  private void recordPlanNodeRelation(PlanNode root, PlanFragmentId planFragmentId) {
    planNodeMap.put(root.getPlanNodeId(), planFragmentId);
    for (PlanNode child : root.getChildren()) {
      recordPlanNodeRelation(child, planFragmentId);
    }
  }
}
