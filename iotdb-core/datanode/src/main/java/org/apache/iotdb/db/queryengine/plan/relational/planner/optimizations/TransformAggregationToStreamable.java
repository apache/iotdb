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

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TableBuiltinScalarFunction;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>Calculate the preGroupedSymbols in {@link AggregationNode}, used in {@link
 * AggregationNode#isStreamable()}.
 *
 * <p>Attention: This optimizer should be used before optimizer of {@link
 * PushAggregationIntoTableScan}, or you can implement {@link
 * Rewriter#visitAggregationTableScan(AggregationTableScanNode, Context)}.
 */
public class TransformAggregationToStreamable implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().getStatement() instanceof Query)
        || !context.getAnalysis().hasAggregates()) {
      return plan;
    }

    return plan.accept(new Rewriter(), new Context(context.getMetadata(), context.sessionInfo()));
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
      Set<Symbol> expectedGroupingKeys;
      if (node.getChild() instanceof ProjectNode) {
        Assignments assignments = ((ProjectNode) node.getChild()).getAssignments();
        expectedGroupingKeys =
            node.getGroupingKeys().stream()
                .map(
                    k -> {
                      Expression v = assignments.get(k);
                      if (!(v instanceof SymbolReference) && isDateBinFunctionOfTime(v)) {
                        return Symbol.of(
                            ((SymbolReference) ((FunctionCall) v).getArguments().get(2)).getName());
                      }
                      return k;
                    })
                .collect(Collectors.toSet());
      } else {
        expectedGroupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
      }

      if (node.getChild()
          .accept(
              new deriveGroupProperties(),
              new GroupContext(context.metadata, context.session, expectedGroupingKeys))) {
        node.setPreGroupedSymbols(node.getGroupingKeys());
      }
      return node;
    }

    @Override
    public PlanNode visitAggregationTableScan(AggregationTableScanNode node, Context context) {
      throw new RuntimeException(
          "This optimizer should be used before optimizer of PushAggregationIntoTableScan");
    }
  }

  private static boolean isDateBinFunctionOfTime(Expression expression) {
    if (expression instanceof FunctionCall) {
      FunctionCall function = (FunctionCall) expression;
      return TableBuiltinScalarFunction.DATE_BIN
              .getFunctionName()
              .equals(function.getName().toString())
          && function.getArguments().get(2) instanceof SymbolReference
          && TimestampOperand.TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(
              ((SymbolReference) function.getArguments().get(2)).getName());
    }
    return false;
  }

  private static class Context {
    private final Metadata metadata;
    private final SessionInfo session;

    public Context(Metadata metadata, SessionInfo session) {
      this.metadata = metadata;
      this.session = session;
    }
  }

  private static class deriveGroupProperties extends PlanVisitor<Boolean, GroupContext> {

    @Override
    public Boolean visitPlan(PlanNode node, GroupContext context) {
      for (PlanNode child : node.getChildren()) {
        if (!child.accept(this, context)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Boolean visitMergeSort(MergeSortNode node, GroupContext context) {
      return node.getChildren().get(0).accept(this, context);
    }

    @Override
    public Boolean visitSort(SortNode node, GroupContext context) {
      Set<Symbol> expectedGroupingKeys = context.groupingKeys;
      List<Symbol> orderKeys = node.getOrderingScheme().getOrderBy();
      if (expectedGroupingKeys.size() < orderKeys.size()) {
        return expectedGroupingKeys.equals(
            orderKeys.stream().limit(expectedGroupingKeys.size()).collect(Collectors.toSet()));
      } else {
        return expectedGroupingKeys.containsAll(orderKeys);
      }
    }

    @Override
    public Boolean visitTableScan(TableScanNode node, GroupContext context) {
      Set<Symbol> expectedGroupingKeys = context.groupingKeys;
      List<Symbol> orderKeys =
          node.getIdAndTimeColumnsInTableStore(context.metadata, context.session);
      // TODO consider date_bin(time) when compare
      if (expectedGroupingKeys.size() < orderKeys.size()) {
        return expectedGroupingKeys.equals(
            orderKeys.stream().limit(expectedGroupingKeys.size()).collect(Collectors.toSet()));
      } else {
        return expectedGroupingKeys.containsAll(orderKeys);
      }
    }

    @Override
    public Boolean visitAggregation(AggregationNode node, GroupContext context) {
      return ImmutableSet.copyOf(node.getGroupingKeys())
          .equals(ImmutableSet.copyOf(context.groupingKeys));
    }

    @Override
    public Boolean visitAggregationTableScan(AggregationTableScanNode node, GroupContext context) {
      throw new RuntimeException(
          "This optimizer should be used before optimizer of PushAggregationIntoTableScan");
    }
  }

  private static class GroupContext {
    private final Metadata metadata;
    private final SessionInfo session;
    private final Set<Symbol> groupingKeys;

    private GroupContext(Metadata metadata, SessionInfo session, Set<Symbol> groupingKeys) {
      this.metadata = metadata;
      this.session = session;
      this.groupingKeys = groupingKeys;
    }
  }
}
