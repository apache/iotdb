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
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.relational.sql.tree.Query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;

public class RelationalDistributionPlanner {
  private final Analysis analysis;
  private final LogicalQueryPlan logicalQueryPlan;
  private final MPPQueryContext mppQueryContext;

  public RelationalDistributionPlanner(
      Analysis analysis, LogicalQueryPlan logicalQueryPlan, MPPQueryContext mppQueryContext) {
    this.analysis = analysis;
    this.logicalQueryPlan = logicalQueryPlan;
    this.mppQueryContext = mppQueryContext;
  }

  public DistributedQueryPlan plan() {
    ExchangeNodeGenerator.PlanContext exchangeContext =
        new ExchangeNodeGenerator.PlanContext(mppQueryContext);
    List<PlanNode> distributedPlanNodeResult =
        new ExchangeNodeGenerator().visitPlan(logicalQueryPlan.getRootNode(), exchangeContext);

    if (distributedPlanNodeResult.size() != 1) {
      throw new IllegalStateException("root node must return only one");
    }

    PlanNode outputNodeWithExchange = distributedPlanNodeResult.get(0);
    if (analysis.getStatement() instanceof Query) {
      analysis
          .getRespDatasetHeader()
          .setColumnToTsBlockIndexMap(
              outputNodeWithExchange.getOutputSymbols().stream()
                  .map(Symbol::getName)
                  .filter(e -> !TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(e))
                  .collect(Collectors.toList()));
    }
    adjustUpStream(outputNodeWithExchange, exchangeContext);

    SubPlan subPlan =
        new SubPlanGenerator()
            .splitToSubPlan(logicalQueryPlan.getContext().getQueryId(), outputNodeWithExchange);
    subPlan.getPlanFragment().setRoot(true);

    List<FragmentInstance> fragmentInstances =
        new FragmentInstanceGenerator(subPlan, analysis, mppQueryContext).plan();

    // Only execute this step for READ operation
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

  private void adjustUpStream(PlanNode root, ExchangeNodeGenerator.PlanContext exchangeContext) {
    if (!exchangeContext.hasExchangeNode) {
      return;
    }

    adjustUpStreamHelper(root, exchangeContext, new HashMap<>());
  }

  private void adjustUpStreamHelper(
      PlanNode root,
      ExchangeNodeGenerator.PlanContext exchangeContext,
      Map<TRegionReplicaSet, IdentitySinkNode> regionNodemap) {
    for (PlanNode child : root.getChildren()) {
      adjustUpStreamHelper(child, exchangeContext, regionNodemap);

      if (child instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) child;
        IdentitySinkNode identitySinkNode =
            regionNodemap.computeIfAbsent(
                exchangeContext
                    .getNodeDistribution(exchangeNode.getChild().getPlanNodeId())
                    .getRegion(),
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
