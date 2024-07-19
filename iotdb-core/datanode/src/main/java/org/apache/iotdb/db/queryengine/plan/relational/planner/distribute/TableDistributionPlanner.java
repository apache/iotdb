/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.WriteFragmentParallelPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushLimitOffsetIntoTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SortElimination;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;

public class TableDistributionPlanner {
  private final Analysis analysis;
  private final LogicalQueryPlan logicalQueryPlan;
  private final MPPQueryContext mppQueryContext;
  private final List<PlanOptimizer> optimizers;

  public TableDistributionPlanner(
      Analysis analysis, LogicalQueryPlan logicalQueryPlan, MPPQueryContext mppQueryContext) {
    this.analysis = analysis;
    this.logicalQueryPlan = logicalQueryPlan;
    this.mppQueryContext = mppQueryContext;
    this.optimizers = Arrays.asList(new PushLimitOffsetIntoTableScan(), new SortElimination());
  }

  public DistributedQueryPlan plan() {

    // generate table model distributed plan
    DistributedPlanGenerator.PlanContext planContext = new DistributedPlanGenerator.PlanContext();
    List<PlanNode> distributedPlanResult =
        new DistributedPlanGenerator(mppQueryContext, analysis)
            .genResult(logicalQueryPlan.getRootNode(), planContext);
    checkArgument(distributedPlanResult.size() == 1, "Root node must return only one");

    // distribute plan optimize rule
    PlanNode distributedPlan = distributedPlanResult.get(0);
    for (PlanOptimizer optimizer : optimizers) {
      distributedPlan =
          optimizer.optimize(
              distributedPlan,
              new PlanOptimizer.Context(
                  null,
                  analysis,
                  null,
                  mppQueryContext,
                  mppQueryContext.getTypeProvider(),
                  new SymbolAllocator(),
                  mppQueryContext.getQueryId(),
                  NOOP,
                  PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
    }

    // add exchange node for distributed plan
    PlanNode outputNodeWithExchange =
        new AddExchangeNodes(mppQueryContext).addExchangeNodes(distributedPlan, planContext);
    if (analysis.getStatement() instanceof Query) {
      analysis
          .getRespDatasetHeader()
          .setColumnToTsBlockIndexMap(outputNodeWithExchange.getOutputColumnNames());
    }
    adjustUpStream(outputNodeWithExchange, planContext);

    // generate subPlan
    SubPlan subPlan =
        new SubPlanGenerator()
            .splitToSubPlan(logicalQueryPlan.getContext().getQueryId(), outputNodeWithExchange);
    subPlan.getPlanFragment().setRoot(true);

    // generate fragment instances
    List<FragmentInstance> fragmentInstances =
        mppQueryContext.getQueryType() == QueryType.READ
            ? new TableModelQueryFragmentPlanner(subPlan, analysis, mppQueryContext).plan()
            : new WriteFragmentParallelPlanner(subPlan, analysis, mppQueryContext).parallelPlan();

    // only execute this step for READ operation
    if (mppQueryContext.getQueryType() == QueryType.READ) {
      setSinkForRootInstance(subPlan, fragmentInstances);
    }

    return new DistributedQueryPlan(
        logicalQueryPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
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

  private void adjustUpStream(PlanNode root, DistributedPlanGenerator.PlanContext context) {
    if (!context.hasExchangeNode) {
      return;
    }

    adjustUpStreamHelper(root, context, new HashMap<>());
  }

  private void adjustUpStreamHelper(
      PlanNode root,
      DistributedPlanGenerator.PlanContext context,
      Map<TRegionReplicaSet, IdentitySinkNode> regionNodeMap) {
    for (PlanNode child : root.getChildren()) {
      adjustUpStreamHelper(child, context, regionNodeMap);

      if (child instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) child;

        IdentitySinkNode identitySinkNode =
            regionNodeMap.computeIfAbsent(
                context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).getRegion(),
                k -> new IdentitySinkNode(mppQueryContext.getQueryId().genPlanNodeId()));
        identitySinkNode.addChild(exchangeNode.getChild());
        identitySinkNode.addDownStreamChannelLocation(
            new DownStreamChannelLocation(exchangeNode.getPlanNodeId().toString()));

        exchangeNode.setChild(identitySinkNode);
        exchangeNode.setIndexOfUpstreamSinkHandle(identitySinkNode.getCurrentLastIndex());
      }
    }
  }
}
