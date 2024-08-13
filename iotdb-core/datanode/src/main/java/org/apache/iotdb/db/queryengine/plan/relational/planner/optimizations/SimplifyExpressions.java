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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.NormalizeOrExpressionRewriter.normalizeOrExpression;

/** <b>Optimization phase:</b> Logical plan planning. */
public class SimplifyExpressions implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    return plan.accept(new Rewriter(), new RewriterContext());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      Expression predicate = extractCommonPredicates(node.getPredicate());
      predicate = normalizeOrExpression(predicate);
      node.setPredicate(predicate);
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      return node;
    }
  }

  private static class RewriterContext {}
}
