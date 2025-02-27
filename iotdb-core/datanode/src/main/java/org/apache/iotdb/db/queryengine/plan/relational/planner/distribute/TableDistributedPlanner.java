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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.WriteFragmentParallelPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DataNodeLocationSupplierFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DistributedOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.DISTRIBUTION_PLANNER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TABLE_TYPE;

public class TableDistributedPlanner {

  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final LogicalQueryPlan logicalQueryPlan;
  private final MPPQueryContext mppQueryContext;
  private final List<PlanOptimizer> optimizers;
  private final Metadata metadata;
  private final DataNodeLocationSupplierFactory.DataNodeLocationSupplier dataNodeLocationSupplier;

  @TestOnly
  public TableDistributedPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      LogicalQueryPlan logicalQueryPlan,
      Metadata metadata,
      DataNodeLocationSupplierFactory.DataNodeLocationSupplier dataNodeLocationSupplier) {
    this(
        analysis,
        symbolAllocator,
        logicalQueryPlan,
        metadata,
        new DistributedOptimizeFactory(new PlannerContext(metadata, new InternalTypeManager()))
            .getPlanOptimizers(),
        dataNodeLocationSupplier);
  }

  public TableDistributedPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      LogicalQueryPlan logicalQueryPlan,
      Metadata metadata,
      List<PlanOptimizer> distributedOptimizers,
      DataNodeLocationSupplierFactory.DataNodeLocationSupplier dataNodeLocationSupplier) {
    this.analysis = analysis;
    this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    this.logicalQueryPlan = logicalQueryPlan;
    this.mppQueryContext = logicalQueryPlan.getContext();
    this.optimizers = distributedOptimizers;
    this.metadata = metadata;
    this.dataNodeLocationSupplier = dataNodeLocationSupplier;
  }

  public DistributedQueryPlan plan() {
    long startTime = System.nanoTime();
    TableDistributedPlanGenerator.PlanContext planContext =
        new TableDistributedPlanGenerator.PlanContext();
    PlanNode outputNodeWithExchange = generateDistributedPlanWithOptimize(planContext);

    if (analysis.isQuery()) {
      analysis
          .getRespDatasetHeader()
          .setTableColumnToTsBlockIndexMap((OutputNode) outputNodeWithExchange);
    }

    adjustUpStream(outputNodeWithExchange, planContext);
    DistributedQueryPlan resultDistributedPlan = generateDistributedPlan(outputNodeWithExchange);

    if (analysis.isQuery()) {
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TABLE_TYPE, DISTRIBUTION_PLANNER, System.nanoTime() - startTime);
    }
    return resultDistributedPlan;
  }

  public PlanNode generateDistributedPlanWithOptimize(
      TableDistributedPlanGenerator.PlanContext planContext) {
    // generate table model distributed plan

    List<PlanNode> distributedPlanResult =
        new TableDistributedPlanGenerator(
                mppQueryContext, analysis, symbolAllocator, dataNodeLocationSupplier)
            .genResult(logicalQueryPlan.getRootNode(), planContext);
    checkArgument(distributedPlanResult.size() == 1, "Root node must return only one");

    // distribute plan optimize rule
    PlanNode distributedPlan = distributedPlanResult.get(0);

    if (analysis.isQuery()) {
      for (PlanOptimizer optimizer : optimizers) {
        distributedPlan =
            optimizer.optimize(
                distributedPlan,
                new PlanOptimizer.Context(
                    mppQueryContext.getSession(),
                    analysis,
                    metadata,
                    mppQueryContext,
                    new SymbolAllocator(),
                    mppQueryContext.getQueryId(),
                    NOOP,
                    PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
      }
    }

    // Add all Symbol Types in SymbolAllocator into TypeProvider
    symbolAllocator
        .getTypes()
        .allTableModelTypes()
        .forEach((k, v) -> mppQueryContext.getTypeProvider().putTableModelType(k, v));

    // add exchange node for distributed plan
    return new AddExchangeNodes(mppQueryContext).addExchangeNodes(distributedPlan, planContext);
  }

  private DistributedQueryPlan generateDistributedPlan(PlanNode outputNodeWithExchange) {
    // generate subPlan
    SubPlan subPlan =
        new SubPlanGenerator()
            .splitToSubPlan(logicalQueryPlan.getContext().getQueryId(), outputNodeWithExchange);
    subPlan.getPlanFragment().setRoot(true);

    // generate fragment instances
    List<FragmentInstance> fragmentInstances =
        mppQueryContext.getQueryType() == QueryType.READ
            ? new TableModelQueryFragmentPlanner(subPlan, analysis, mppQueryContext).plan()
            : new WriteFragmentParallelPlanner(
                    subPlan, analysis, mppQueryContext, WritePlanNode::splitByPartition)
                .parallelPlan();

    // only execute this step for READ operation
    if (mppQueryContext.getQueryType() == QueryType.READ) {
      setSinkForRootInstance(subPlan, fragmentInstances);
    }

    return new DistributedQueryPlan(subPlan, fragmentInstances);
  }

  public void setSinkForRootInstance(SubPlan subPlan, List<FragmentInstance> instances) {
    FragmentInstance rootInstance = null;
    for (FragmentInstance instance : instances) {
      if (instance.getFragment().getId().equals(subPlan.getPlanFragment().getId())) {
        rootInstance = instance;
        break;
      }
    }
    // root should not be null during normal process
    if (rootInstance == null) {
      return;
    }

    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            mppQueryContext.getQueryId().genPlanNodeId(),
            Collections.singletonList(rootInstance.getFragment().getPlanNodeTree()),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    mppQueryContext.getLocalDataBlockEndpoint(),
                    mppQueryContext
                        .getResultNodeContext()
                        .getVirtualFragmentInstanceId()
                        .toThrift(),
                    mppQueryContext.getResultNodeContext().getVirtualResultNodeId().getId())));
    mppQueryContext
        .getResultNodeContext()
        .setUpStream(
            rootInstance.getHostDataNode().mPPDataExchangeEndPoint,
            rootInstance.getId(),
            sinkNode.getPlanNodeId());
    rootInstance.getFragment().setPlanNodeTree(sinkNode);
  }

  private void adjustUpStream(PlanNode root, TableDistributedPlanGenerator.PlanContext context) {
    if (!context.hasExchangeNode) {
      return;
    }

    adjustUpStreamHelper(root, context, new HashMap<>());
  }

  private void adjustUpStreamHelper(
      PlanNode root,
      TableDistributedPlanGenerator.PlanContext context,
      Map<TRegionReplicaSet, IdentitySinkNode> regionNodeMap) {
    for (PlanNode child : root.getChildren()) {
      adjustUpStreamHelper(child, context, regionNodeMap);

      if (child instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) child;

        //        IdentitySinkNode identitySinkNode =
        //            regionNodeMap.computeIfAbsent(
        //
        // context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).getRegion(),
        //                k -> new IdentitySinkNode(mppQueryContext.getQueryId().genPlanNodeId()));

        // In table model, each ExchangeNode matches only one IdentitySinkNode
        IdentitySinkNode identitySinkNode =
            new IdentitySinkNode(mppQueryContext.getQueryId().genPlanNodeId());
        identitySinkNode.addChild(exchangeNode.getChild());
        identitySinkNode.addDownStreamChannelLocation(
            new DownStreamChannelLocation(exchangeNode.getPlanNodeId().toString()));

        exchangeNode.setChild(identitySinkNode);
        exchangeNode.setIndexOfUpstreamSinkHandle(identitySinkNode.getCurrentLastIndex());
      }
    }
  }
}
