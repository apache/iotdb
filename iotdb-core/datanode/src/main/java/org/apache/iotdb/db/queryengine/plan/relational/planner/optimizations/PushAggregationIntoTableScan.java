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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode.combineAggregationAndTableScan;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>The Aggregation may be pushed down to the TableScanNode, so that we can make use of
 * statistics.
 *
 * <p>Attention: This optimizer depends on {@link UnaliasSymbolReferences}
 */
public class PushAggregationIntoTableScan implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().getStatement() instanceof Query)
        || !context.getAnalysis().hasAggregates()) {
      return plan;
    }

    return plan.accept(
        new Rewriter(),
        new Context(
            context.getQueryContext().getQueryId(), context.getMetadata(), context.sessionInfo()));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Context> {

    @Override
    public PlanNode visitPlan(PlanNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Context context) {
      TableScanNode tableScanNode = null;
      ProjectNode projectNode = null;
      if (node.getChild() instanceof TableScanNode) {
        tableScanNode = (TableScanNode) node.getChild();
      }
      if (node.getChild() instanceof ProjectNode) {
        projectNode = (ProjectNode) node.getChild();
        if (projectNode.getChild() instanceof TableScanNode) {
          tableScanNode = (TableScanNode) projectNode.getChild();
        }
      }
      if (tableScanNode == null) { // no need to optimize
        return node;
      }

      int pushDownLevel =
          calculatePushDownLevel(
              node.getAggregations().values(),
              node.getGroupingKeys(),
              projectNode,
              tableScanNode,
              context.session,
              context.metadata);
      if (pushDownLevel == 0) { // no push-down
        return node;
      } else if (pushDownLevel == 1) { // partial push-down
        checkArgument(projectNode == null, "Unexpected ProjectNode in push-down");
        AggregationTableScanNode aggregationTableScanNode =
            combineAggregationAndTableScan(
                context.queryId.genPlanNodeId(), node, tableScanNode, AggregationNode.Step.PARTIAL);
        return new AggregationNode(
            node.getPlanNodeId(),
            aggregationTableScanNode,
            node.getAggregations(),
            node.getGroupingSets(),
            node.getPreGroupedSymbols(),
            AggregationNode.Step.FINAL,
            node.getHashSymbol(),
            node.getHashSymbol());
      } else { // complete push-down
        checkArgument(projectNode == null, "Unexpected ProjectNode in push-down");
        return combineAggregationAndTableScan(context.queryId.genPlanNodeId(), node, tableScanNode);
      }
    }

    private int calculatePushDownLevel(
        Collection<AggregationNode.Aggregation> values,
        List<Symbol> groupingKeys,
        ProjectNode projectNode,
        TableScanNode tableScanNode,
        SessionInfo session,
        Metadata metadata) {
      final int[] currPushDownLevel = {2};
      boolean hasProject = projectNode != null;
      Map<Symbol, Expression> assignments =
          hasProject ? projectNode.getAssignments().getMap() : null;
      // calculate Function part
      for (AggregationNode.Aggregation aggregation : values) {
        // if the function cannot make use of Statistics, we don't push down
        if (!metadata.canUseStatistics(
            aggregation.getResolvedFunction().getSignature().getName())) {
          currPushDownLevel[0] = 0;
          break;
        }

        // if expr appears in arguments of Aggregation, we don't push down
        if (hasProject
            && aggregation.getArguments().stream()
                .anyMatch(
                    argument ->
                        !(assignments.get(Symbol.from(argument)) instanceof SymbolReference))) {
          currPushDownLevel[0] = 0;
          break;
        }
      }

      // calculate DataSet part
      if (groupingKeys.stream()
          .anyMatch(
              groupingKey ->
                  hasProject
                          && !(assignments.get(groupingKey) instanceof SymbolReference
                              || isDataBinFunctionOfTime(assignments.get(groupingKey)))
                      || tableScanNode.isIdColumn( // TODO
                          groupingKey))) { // if expr except data_bin(time) or Measurement column
        // appears in groupingKeys, we don't push down

        currPushDownLevel[0] = 0;
      } else if (ImmutableSet.copyOf(groupingKeys)
          .containsAll(
              tableScanNode.getIdColumnsInTableStore(
                  metadata, session))) { // if all ID columns appear in groupingKeys and no
        // Measurement column appears, we can push down completely
        currPushDownLevel[0] = Math.min(currPushDownLevel[0], 2);
      } else {
        currPushDownLevel[0] = Math.min(currPushDownLevel[0], 1);
      }
      return currPushDownLevel[0];
    }

    private boolean isDataBinFunctionOfTime(Expression expression) {
      if (expression instanceof FunctionCall) {
        FunctionCall function = (FunctionCall) expression;
        return function.getName().toString().equals("data_bin");
      }
      return false;
    }
  }

  private static class Context {
    private final QueryId queryId;
    private final Metadata metadata;
    private final SessionInfo session;

    public Context(QueryId queryId, Metadata metadata, SessionInfo session) {
      this.queryId = queryId;
      this.metadata = metadata;
      this.session = session;
    }
  }
}
