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
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateCombineIntoTableScanChecker;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;

/**
 * After the optimized rule {@link SimplifyExpressions} finished, predicate expression in FilterNode
 * has been transformed to conjunctive normal forms(CNF).
 *
 * <p>In this class, we examine each expression in CNFs, determine how to use it, in metadata query,
 * or pushed down into ScanOperators, or it can only be used in FilterNode above with TableScanNode.
 *
 * <p>Notice that, when aggregation, multi-table, join are introduced, this optimization rule need
 * to be adapted.
 */
public class FilterScanCombine implements RelationalPlanOptimizer {

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      IPartitionFetcher partitionFetcher,
      SessionInfo sessionInfo,
      MPPQueryContext queryContext) {

    if (!analysis.hasValueFilter()) {
      return planNode;
    }

    return planNode.accept(new Rewriter(), new RewriterContext(queryContext));
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
      context.filterNode = node;

      if (node.getPredicate() != null) {
        // when exist diff function, predicate can not be pushed down
        if (containsDiffFunction(node.getPredicate())) {
          context.pushDownPredicate = null;
          return node;
        }

        context.pushDownPredicate = node.getPredicate();
        return node.getChild().accept(this, context);
      } else {
        throw new IllegalStateException(
            "Filter node has no predicate, node: " + node.getPlanNodeId());
      }
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      // has diff in FilterNode
      if (context.pushDownPredicate == null) {
        node.setPushDownPredicate(null);
        return node;
      }

      context.queryContext.setTableModelPredicateExpressions(
          splitConjunctionExpressions(context, node));

      // exist expressions can push down to scan operator
      if (!context.queryContext.getTableModelPredicateExpressions().get(1).isEmpty()) {
        List<Expression> expressions =
            context.queryContext.getTableModelPredicateExpressions().get(1);
        node.setPushDownPredicate(
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions));
      } else {
        node.setPushDownPredicate(null);
      }

      // exist expressions can not push down to scan operator
      if (!context.queryContext.getTableModelPredicateExpressions().get(2).isEmpty()) {
        List<Expression> expressions =
            context.queryContext.getTableModelPredicateExpressions().get(2);
        return new FilterNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node,
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions));
      }

      return node;
    }
  }

  private static List<List<Expression>> splitConjunctionExpressions(
      RewriterContext context, TableScanNode node) {
    Expression predicate = context.pushDownPredicate;

    Set<String> idOrAttributeColumnNames =
        node.getIdAndAttributeIndexMap().keySet().stream()
            .map(Symbol::getName)
            .collect(Collectors.toSet());

    Set<String> measurementColumnNames =
        node.getAssignments().entrySet().stream()
            .filter(e -> MEASUREMENT.equals(e.getValue().getColumnCategory()))
            .map(e -> e.getKey().getName())
            .collect(Collectors.toSet());

    List<Expression> metadataExpressions = new ArrayList<>();
    List<Expression> expressionsCanPushDown = new ArrayList<>();
    List<Expression> expressionsCannotPushDown = new ArrayList<>();

    if (predicate instanceof LogicalExpression
        && ((LogicalExpression) predicate).getOperator() == LogicalExpression.Operator.AND) {

      for (Expression expression : ((LogicalExpression) predicate).getTerms()) {
        if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, expression)) {
          metadataExpressions.add(expression);
        } else if (PredicateCombineIntoTableScanChecker.check(measurementColumnNames, expression)) {
          expressionsCanPushDown.add(expression);
        } else {
          expressionsCannotPushDown.add(expression);
        }
      }

      return Arrays.asList(metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
    }

    if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, predicate)) {
      metadataExpressions.add(predicate);
    } else if (PredicateCombineIntoTableScanChecker.check(measurementColumnNames, predicate)) {
      expressionsCanPushDown.add(predicate);
    } else {
      expressionsCannotPushDown.add(predicate);
    }

    return Arrays.asList(metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
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
    MPPQueryContext queryContext;
    FilterNode filterNode;

    public RewriterContext(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
    }
  }
}
