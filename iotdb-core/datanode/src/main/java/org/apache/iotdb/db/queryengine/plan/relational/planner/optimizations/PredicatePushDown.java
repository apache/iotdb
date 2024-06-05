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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Node;

import java.util.ArrayList;
import java.util.List;

/** Push down predicate to TableScanNode as possible. */
public class PredicatePushDown implements RelationalPlanOptimizer {

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      IPartitionFetcher partitionFetcher,
      SessionInfo sessionInfo,
      MPPQueryContext context) {

    if (!analysis.hasValueFilter()) {
      return planNode;
    }

    return planNode.accept(new Rewriter(), new RewriterContext());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      throw new IllegalArgumentException(
          String.format("Unexpected plan node: %s in TableModel PredicatePushDown", node));
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, RewriterContext context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      if (node.getPredicate() != null) {
        // when exist diff function, predicate can not be pushed down
        if (containsDiffFunction(node.getPredicate())) {
          return node;
        }

        context.pushDownPredicate = node.getPredicate();
        node.getChild().accept(this, context);

        // remove FilterNode after push down
        return node.getChild();
      }

      node.getChild().accept(this, context);
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      node.setPushDownPredicate(context.pushDownPredicate);
      return node;
    }
  }

  static boolean containsDiffFunction(Expression expression) {
    if (expression instanceof FunctionCall
        && "diff".equalsIgnoreCase(((FunctionCall) expression).getName().toString())) {
      return true;
    }

    if (!expression.getChildren().isEmpty()) {
      for (Node node : expression.getChildren()) {
        if (containsDiffFunction((Expression) node)) {
          return true;
        }
      }
    }

    return false;
  }

  private static class RewriterContext {
    Expression pushDownPredicate;
  }
}
